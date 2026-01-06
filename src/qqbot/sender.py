"""
QQ消息发送器
"""
import json
import time
import uuid
import threading
import asyncio
from typing import Any
from pathlib import Path
from dataclasses import dataclass, asdict
from loguru import logger

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# 全局变量，确保只有一个BotClient实例
_global_bot_client = None
_global_api = None
_global_lock = threading.Lock()
_global_connection_time = 0
_global_connection_valid = False


@dataclass
class SendResult:
    """发送结果数据类"""
    message_id: str
    success: bool
    target_id: str
    target_type: str
    message: str
    api_result: str = ""
    content_length: int = 0
    elapsed_time: float = 0.0
    timestamp: float = 0.0
    error: str = ""
    metadata: dict[str, str] | None = None


@dataclass
class HealthCheckResult:
    """健康检查结果数据类"""
    healthy: bool
    message: str
    connected: bool
    user_id: str = ""
    nickname: str = ""
    timestamp: float = 0.0
    error: str = ""


class QQBotSender:
    """
    QQ消息发送器

    封装ncatbot的QQ消息发送功能，提供统一的接口
    """

    def __init__(self):
        """初始化QQ消息发送器"""
        self._client = None
        self._api = None
        self._connected = False
        self._connection_lock = threading.Lock()
        self._last_error_time = 0.0
        self._error_count = 0
        self._max_retries = 3

    def _import_ncatbot(self) -> tuple[bool, Exception | None]:
        """导入ncatbot模块"""
        try:
            global _global_bot_client, _global_api, _global_connection_valid

            with _global_lock:
                if _global_bot_client is None:
                    from ncatbot.core import BotClient
                    _global_bot_client = BotClient()
                    logger.info("✅ 创建全局BotClient实例")

                if _global_api is None:
                    # 延迟创建API，避免过早启动bot
                    pass

            return True, None

        except ImportError as e:
            logger.error(f"❌ 导入ncatbot失败: {e}")
            return False, e
        except Exception as e:
            logger.error(f"💥 初始化ncatbot失败: {e}")
            return False, e

    async def connect(self) -> bool:
        """
        连接到QQBot

        Returns:
            是否连接成功
        """
        if self._connected:
            return True

        with self._connection_lock:
            if self._connected:  # 双重检查
                return True

            try:
                logger.info("🔗 连接到QQBot...")

                # 检查ncatbot是否可用
                success, error = self._import_ncatbot()
                if not success:
                    return False

                global _global_bot_client, _global_api, _global_connection_valid, _global_connection_time

                # 如果已经连接且有效，直接使用
                if (_global_api is not None and
                    _global_connection_valid and
                    time.time() - _global_connection_time < 300):  # 5分钟内有效的连接

                    self._api = _global_api
                    self._connected = True
                    logger.info("✅ 使用现有的有效QQBot连接")
                    return True

                # 创建或重新创建API连接
                with _global_lock:
                    if _global_bot_client is None:
                        from ncatbot.core import BotClient
                        _global_bot_client = BotClient()

                    # 使用try-except保护run_backend调用
                    try:
                        _global_api = _global_bot_client.run_backend()
                        _global_connection_time = time.time()

                        # 测试连接
                        test_result = _global_api.get_login_info_sync()

                        if test_result and hasattr(test_result, 'user_id'):
                            _global_connection_valid = True
                            self._api = _global_api
                            self._connected = True

                            user_id = getattr(test_result, 'user_id', '未知')
                            nickname = getattr(test_result, 'nickname', '未知')
                            logger.info(f"✅ QQBot连接成功: {nickname}({user_id})")
                            return True
                        else:
                            logger.error("❌ QQBot连接测试失败: 无法获取登录信息")
                            _global_connection_valid = False
                            return False

                    except Exception as e:
                        logger.error(f"❌ 创建QQBot API连接失败: {e}")
                        _global_connection_valid = False
                        return False

            except Exception as e:
                logger.error(f"💥 QQBot连接失败: {e}")
                import traceback
                logger.error(f"详细错误: {traceback.format_exc()}")
                return False

    async def disconnect(self):
        """断开连接"""
        with self._connection_lock:
            try:
                # 我们不真正断开全局连接，只重置本地状态
                self._connected = False
                self._error_count = 0
                logger.info("🔌 QQBot连接状态已重置")

            except Exception as e:
                logger.error(f"断开连接失败: {e}")

    def _create_single_segment(self, segment_data: dict[str, Any]) -> Any:
        """
        根据消息段数据创建单个消息段

        Args:
            segment_data: 消息段数据

        Returns:
            消息段对象或None
        """
        try:
            from ncatbot.core import Text, Face, Image, At, Reply

            segment_type = segment_data.get("type", "").lower()
            data = segment_data.get("data", {})

            match segment_type:
                case "text":
                    if isinstance(data, str):
                        return Text(data)
                    elif isinstance(data, dict):
                        return Text(data.get("text", ""))

                case "face":
                    match data:
                        case int() | str():
                            return Face(str(data))
                        case dict():
                            return Face(
                                id=str(data.get("id", "")),
                                faceText=data.get("faceText", "[表情]")
                            )

                case "image":
                    match data:
                        case str():
                            return Image(data)
                        case dict():
                            return Image(
                                url=data.get("url", ""),
                                summary=data.get("summary", "[图片]"),
                                sub_type=data.get("sub_type", 0)
                            )

                case "at":
                    match data:
                        case str():
                            return At(qq=data)
                        case dict():
                            return At(qq=str(data.get("qq", "")))

                case "reply":
                    match data:
                        case int() | str():
                            return Reply(id=str(data))
                        case dict():
                            return Reply(id=str(data.get("id", "")))

                case _:
                    logger.warning(f"⚠️ 不支持的消息段类型: {segment_type}")
                    return None

        except Exception as e:
            logger.error(f"💥 创建消息段失败: {e}")
            return None

    def _create_message_array(self, content_data: str | list | dict) -> Any:
        """
        根据内容数据创建消息对象

        Args:
            content_data: 内容数据

        Returns:
            MessageArray对象或None
        """
        try:
            from ncatbot.core import MessageArray, Text

            match content_data:
                case str():
                    # 简单文本消息
                    return MessageArray([Text(content_data)])

                case dict():
                    content_type = content_data.get("type", "").lower()
                    data = content_data.get("data", [])

                    match content_type:
                        case "messagearray" | "messagechain" if isinstance(data, list):
                            # 处理MessageArray/MessageChain
                            segments: list[Any] = []
                            for item in data:
                                match item:
                                    case str():
                                        segments.append(Text(item))
                                    case dict():
                                        segment = self._create_single_segment(item)
                                        if segment:
                                            segments.append(segment)

                            if segments:
                                return MessageArray(segments)

                        case _:
                            # 单个消息段
                            segment = self._create_single_segment(content_data)
                            if segment:
                                return MessageArray([segment])

                case list():
                    # 列表形式的多个消息段
                    segments: list[Any] = []
                    for item in content_data:
                        match item:
                            case str():
                                segments.append(Text(item))
                            case dict():
                                segment = self._create_single_segment(item)
                                if segment:
                                    segments.append(segment)

                    if segments:
                        return MessageArray(segments)

            logger.warning("⚠️ 无法解析的消息内容格式")
            return MessageArray([Text("")])

        except Exception as e:
            logger.error(f"💥 创建MessageArray失败: {e}")
            return MessageArray([Text("")])

    async def _safe_send_message(self, message_array: Any, target_id: str,
                                target_type: str) -> tuple[bool, str, Any]:
        """
        安全发送消息，处理异常

        Args:
            message_array: 消息对象
            target_id: 目标ID
            target_type: 目标类型

        Returns:
            (是否成功, 消息, API结果)
        """
        try:
            if target_type.lower() == "user":
                # 发送私聊消息
                result = self._api.post_private_msg_sync(
                    user_id=target_id,
                    rtf=message_array
                )
                return True, "私聊消息发送成功", result

            elif target_type.lower() == "group":
                # 发送群聊消息
                result = self._api.post_group_msg_sync(
                    group_id=target_id,
                    rtf=message_array
                )
                return True, "群聊消息发送成功", result

            else:
                return False, f"不支持的目标类型: {target_type}", None

        except Exception as e:
            logger.error(f"❌ 发送消息API调用失败: {e}")
            return False, f"发送失败: {str(e)}", None

    async def send_message(
        self,
        target_id: str,
        target_type: str,
        content: str | list | dict,
        metadata: dict[str, Any] | None = None
    ) -> SendResult:
        """
        发送QQ消息

        Args:
            target_id: 目标ID（QQ号或群号）
            target_type: 目标类型: "user" 或 "group"
            content: 消息内容
            metadata: 元数据

        Returns:
            发送结果
        """
        start_time = time.time()
        message_id = str(uuid.uuid4())
        metadata = metadata or {}

        # 验证参数
        if not target_id or not target_type:
            elapsed_time = time.time() - start_time
            return SendResult(
                message_id=message_id,
                success=False,
                target_id=target_id or "",
                target_type=target_type or "",
                message="参数错误: target_id和target_type不能为空",
                content_length=len(str(content)),
                elapsed_time=round(elapsed_time, 2),
                timestamp=time.time(),
                metadata=metadata
            )

        # 尝试连接
        if not await self.connect():
            elapsed_time = time.time() - start_time
            return SendResult(
                message_id=message_id,
                success=False,
                target_id=target_id,
                target_type=target_type,
                message="QQBot连接失败",
                content_length=len(str(content)),
                elapsed_time=round(elapsed_time, 2),
                timestamp=time.time(),
                metadata=metadata
            )

        # 创建消息对象
        message_array = self._create_message_array(content)
        if message_array is None:
            elapsed_time = time.time() - start_time
            return SendResult(
                message_id=message_id,
                success=False,
                target_id=target_id,
                target_type=target_type,
                message="消息内容格式错误",
                content_length=len(str(content)),
                elapsed_time=round(elapsed_time, 2),
                timestamp=time.time(),
                metadata=metadata
            )

        # 尝试发送消息，支持重试
        for attempt in range(self._max_retries):
            try:
                if attempt > 0:
                    logger.info(f"🔄 第{attempt + 1}次重试发送消息...")
                    await asyncio.sleep(attempt * 0.5)  # 指数退避

                success, message, api_result = await self._safe_send_message(
                    message_array, target_id, target_type
                )

                if success:
                    elapsed_time = time.time() - start_time
                    self._error_count = 0  # 重置错误计数

                    return SendResult(
                        message_id=message_id,
                        success=True,
                        target_id=target_id,
                        target_type=target_type,
                        message=message,
                        api_result=str(api_result) if api_result else "",
                        content_length=len(str(content)),
                        elapsed_time=round(elapsed_time, 2),
                        timestamp=time.time(),
                        metadata=metadata
                    )
                else:
                    # 发送失败，标记连接无效
                    self._connected = False
                    global _global_connection_valid
                    _global_connection_valid = False

            except Exception as e:
                logger.error(f"❌ 发送消息异常 (尝试 {attempt + 1}/{self._max_retries}): {e}")
                self._connected = False
                _global_connection_valid = False

        # 所有重试都失败
        elapsed_time = time.time() - start_time
        self._error_count += 1
        self._last_error_time = time.time()

        return SendResult(
            message_id=message_id,
            success=False,
            target_id=target_id,
            target_type=target_type,
            message=f"发送失败，已尝试{self._max_retries}次",
            content_length=len(str(content)),
            elapsed_time=round(elapsed_time, 2),
            timestamp=time.time(),
            error="发送重试次数耗尽",
            metadata=metadata
        )

    async def send_batch_messages(
        self,
        target_ids: list[str],
        target_type: str,
        content: str | list | dict,
        metadata: dict[str, Any] | None = None
    ) -> dict[str, Any]:
        """
        批量发送QQ消息

        Args:
            target_ids: 目标ID列表
            target_type: 目标类型: "user" 或 "group"
            content: 消息内容
            metadata: 元数据

        Returns:
            批量发送结果
        """
        batch_id = str(uuid.uuid4())
        start_time = time.time()
        metadata = metadata or {}

        logger.info(f"📦 批量发送QQ消息，数量: {len(target_ids)}")
        logger.info(f"🎯 目标类型: {target_type}")

        # 先创建消息对象
        message_array = self._create_message_array(content)
        if message_array is None:
            elapsed_time = time.time() - start_time
            return {
                "batch_id": batch_id,
                "success": False,
                "message": "消息内容格式错误",
                "total_count": len(target_ids),
                "success_count": 0,
                "failure_count": len(target_ids),
                "elapsed_time": round(elapsed_time, 2),
                "timestamp": time.time(),
                "metadata": metadata
            }

        # 确保连接
        if not await self.connect():
            elapsed_time = time.time() - start_time
            return {
                "batch_id": batch_id,
                "success": False,
                "message": "QQBot连接失败",
                "total_count": len(target_ids),
                "success_count": 0,
                "failure_count": len(target_ids),
                "elapsed_time": round(elapsed_time, 2),
                "timestamp": time.time(),
                "metadata": metadata
            }

        # 并行发送
        results: list[dict[str, Any]] = []
        success_count = 0
        failure_count = 0

        for i, target_id in enumerate(target_ids):
            try:
                logger.debug(f"   [{i+1}/{len(target_ids)}] 发送到: {target_type} {target_id}")

                # 发送单条消息
                send_result = await self._safe_send_message(
                    message_array, target_id, target_type
                )

                success, message, api_result = send_result

                results.append({
                    "target_id": target_id,
                    "success": success,
                    "message": message,
                    "api_result": str(api_result) if api_result else ""
                })

                if success:
                    success_count += 1
                else:
                    failure_count += 1
                    # 发送失败时，在下一次发送前重置连接
                    if i < len(target_ids) - 1:  # 不是最后一个
                        self._connected = False
                        _global_connection_valid = False

            except Exception as e:
                logger.error(f"批量发送失败 - {target_type} {target_id}: {e}")

                results.append({
                    "target_id": target_id,
                    "success": False,
                    "message": f"发送异常: {str(e)}",
                    "error": str(e)
                })
                failure_count += 1
                self._connected = False
                _global_connection_valid = False

        elapsed_time = time.time() - start_time

        batch_result = {
            "batch_id": batch_id,
            "success": success_count > 0,
            "target_type": target_type,
            "total_count": len(target_ids),
            "success_count": success_count,
            "failure_count": failure_count,
            "success_rate": success_count / len(target_ids) if target_ids else 0.0,
            "results": results,
            "content": str(content),
            "content_length": len(str(content)),
            "elapsed_time": round(elapsed_time, 2),
            "timestamp": time.time(),
            "metadata": metadata
        }

        logger.info(f"📊 批量发送完成: 成功 {success_count} 条，失败 {failure_count} 条 ({elapsed_time:.2f}s)")

        return batch_result

    async def health_check(self) -> HealthCheckResult:
        """
        健康检查

        Returns:
            健康状态
        """
        try:
            # 检查ncatbot是否可用
            success, error = self._import_ncatbot()
            if not success:
                return HealthCheckResult(
                    healthy=False,
                    message=f"ncatbot模块不可用: {str(error)}",
                    connected=False,
                    timestamp=time.time(),
                    error=str(error) if error else ""
                )

            # 尝试连接
            if not await self.connect():
                return HealthCheckResult(
                    healthy=False,
                    message="QQBot连接失败",
                    connected=False,
                    timestamp=time.time()
                )

            # 获取登录信息
            try:
                login_info = self._api.get_login_info_sync()

                if login_info and hasattr(login_info, 'user_id'):
                    user_id = getattr(login_info, 'user_id', '未知')
                    nickname = getattr(login_info, 'nickname', '未知')

                    # 更新全局连接状态
                    global _global_connection_valid, _global_connection_time
                    _global_connection_valid = True
                    _global_connection_time = time.time()

                    return HealthCheckResult(
                        healthy=True,
                        message="QQBot运行正常",
                        connected=True,
                        user_id=user_id,
                        nickname=nickname,
                        timestamp=time.time()
                    )
                else:
                    return HealthCheckResult(
                        healthy=False,
                        message="QQBot状态异常",
                        connected=True,
                        timestamp=time.time()
                    )

            except Exception as e:
                logger.error(f"获取登录信息失败: {e}")
                self._connected = False
                _global_connection_valid = False

                return HealthCheckResult(
                    healthy=False,
                    message=f"获取登录信息失败: {str(e)}",
                    connected=False,
                    timestamp=time.time(),
                    error=str(e)
                )

        except Exception as e:
            logger.error(f"健康检查失败: {e}")
            return HealthCheckResult(
                healthy=False,
                message=f"健康检查失败: {str(e)}",
                connected=False,
                timestamp=time.time(),
                error=str(e)
            )

    def get_status(self) -> dict[str, Any]:
        """
        获取发送器状态

        Returns:
            状态信息
        """
        return {
            "connected": self._connected,
            "error_count": self._error_count,
            "last_error_time": self._last_error_time,
            "max_retries": self._max_retries,
            "global_connection_valid": _global_connection_valid,
            "global_connection_time": _global_connection_time
        }
