"""
QQBot微服务主逻辑
"""
import asyncio
import socket
import sys
import os
from pathlib import Path
from typing import Optional
from dataclasses import asdict
from loguru import logger

import sys
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.common.config import ConfigManager
from src.common.consul import ConsulClient
from src.qqbot.sender import QQBotSender, HealthCheckResult
from src.qqbot.server import create_server


class QQBotMicroservice:
    def __init__(self, config_path: Path):
        self.config_path = config_path
        self.config: Optional[ConfigManager] = None
        self.consul_client: Optional[ConsulClient] = None
        self.sender: Optional[QQBotSender] = None
        self.grpc_server = None
        self._shutting_down = False
        self._tasks = []

    async def start(self) -> bool:
        try:
            logger.info("🚀 启动 QQBot 微服务...")

            # 加载配置
            self.config = ConfigManager()
            if not await self.config.load(self.config_path):
                logger.error("❌ 配置加载失败")
                return False

            cfg = self.config.get()

            # 设置日志
            await self._setup_logging(cfg.log)

            # 打印配置
            await self._print_config(cfg)

            # 初始化QQBot发送器
            logger.info("🤖 初始化QQBot发送器...")
            self.sender = QQBotSender()

            # 连接到QQBot
            if not await self.sender.connect():
                logger.error("❌ QQBot连接失败")
                return False

            # 解析监听地址
            host, port_str = cfg.server.listen_on.split(":")
            port = int(port_str)

            if host in ["0.0.0.0", "127.0.0.1", "[::]", "[::1]"]:
                host = socket.gethostbyname(socket.gethostname())

            # Consul注册
            if cfg.consul.host:
                logger.info(f"🔗 连接到 Consul: {cfg.consul.host}")

                # 准备KV数据（接口定义）
                server_data = {
                    "fields": {
                        "target_id": {
                            "type": "str",
                            "description": "目标ID（QQ号或群号）",
                            "required": True
                        },
                        "target_type": {
                            "type": "enum",
                            "description": "目标类型：user（私聊）或 group（群聊）",
                            "required": True,
                            "enum": ["user", "group"]
                        },
                        "content": {
                            "type": "dict",
                            "description": "消息内容，支持多种格式",
                            "required": True
                        },
                        "metadata": {
                            "type": "dict",
                            "description": "附加元数据",
                            "required": False
                        },
                        "sender_id": {
                            "type": "str",
                            "description": "发送者标识",
                            "required": False
                        }
                    }
                }

                # 准备元数据
                meta = {
                    "version": "1.0.0",
                    "host": socket.gethostname(),
                    "pid": str(os.getpid()),
                    "qq_bot": "ncatbot",
                    "features": "private_message,group_message,rich_message"
                }

                # 创建Consul客户端
                self.consul_client = ConsulClient(
                    host=cfg.consul.host,
                    token=cfg.consul.token,
                    scheme=cfg.consul.scheme
                )

                # 注册服务
                if await self.consul_client.register_service(
                    service_name=cfg.server.name,
                    address=host,
                    port=port,
                    service_desc="基于ncatbot的QQ机器人消息发送微服务",
                    server_data=server_data,
                    meta=meta
                ):
                    logger.info("✅ Consul 注册成功")
                else:
                    logger.warning("⚠️ Consul 注册失败，服务继续运行")

            # 创建gRPC服务器
            logger.info("🌐 创建 gRPC 服务器...")
            self.grpc_server = create_server(
                sender=self.sender,
                max_workers=cfg.server.max_workers
            )

            # 添加监听端口
            self.grpc_server.add_insecure_port(cfg.server.listen_on)
            await self.grpc_server.start()

            logger.info(f"✅ gRPC 服务器启动在 {cfg.server.listen_on}")
            logger.info(f"🤖 服务名称: {cfg.server.name}")

            # 获取QQBot信息
            try:
                health_info = await self.sender.health_check()
                if health_info.healthy:
                    logger.info(f"👤 QQ账号: {health_info.user_id}")
                    logger.info(f"🏷️  昵称: {health_info.nickname}")
            except Exception as e:
                logger.warning(f"⚠️ 获取QQBot信息失败: {e}")

            # 启动健康检查任务
            self._tasks.append(
                asyncio.create_task(self._health_check_task())
            )

            logger.info("🎉 QQBot 微服务启动完成！")
            return True

        except Exception as e:
            logger.error(f"❌ 服务启动失败: {e}")
            import traceback
            logger.error(f"详细错误: {traceback.format_exc()}")
            return False

    async def _health_check_task(self):
        """健康检查任务"""
        check_count = 0
        max_failures = 3

        try:
            while not self._shutting_down:
                await asyncio.sleep(30)
                check_count += 1

                if self.sender:
                    try:
                        health_info = await self.sender.health_check()

                        if not health_info.healthy:
                            logger.warning(f"⚠️ 健康检查: QQBot连接异常 - {health_info.message}")

                            # 如果是第3次连续失败，尝试重新连接
                            if check_count % 3 == 0:
                                logger.info("🔄 尝试重新连接QQBot...")
                                if hasattr(self.sender, '_connected'):
                                    self.sender._connected = False
                                await self.sender.connect()
                                check_count = 0
                        else:
                            check_count = 0
                            if check_count % 5 == 0:
                                logger.debug(f"📊 QQBot状态正常: {health_info.message}")

                    except Exception as e:
                        logger.error(f"健康检查异常: {e}")
                        check_count += 1

                        if check_count >= max_failures:
                            logger.warning("🔄 健康检查多次失败，尝试重置连接...")
                            if hasattr(self.sender, '_connected'):
                                self.sender._connected = False
                            check_count = 0

        except asyncio.CancelledError:
            logger.debug("健康检查任务被取消")
        except Exception as e:
            logger.error(f"健康检查任务异常: {e}")

    async def run(self):
        """运行服务"""
        try:
            await self.grpc_server.wait_for_termination()

        except asyncio.CancelledError:
            logger.info("服务任务被取消")
        except Exception as e:
            logger.error(f"gRPC服务器异常: {e}")

    async def stop(self):
        """停止服务 - 保证优雅关闭和清理"""
        if self._shutting_down:
            return

        self._shutting_down = True
        logger.info("🛑 停止 QQBot 微服务...")

        # 1. 取消所有后台任务
        for task in self._tasks:
            if not task.done():
                task.cancel()

        # 2. 等待任务结束（有超时）
        if self._tasks:
            try:
                logger.info("⏳ 等待任务停止...")
                await asyncio.wait_for(
                    asyncio.gather(*self._tasks, return_exceptions=True),
                    timeout=3.0
                )
                logger.info("✅ 所有任务已停止")
            except asyncio.TimeoutError:
                logger.warning("⏰ 等待任务停止超时，强制继续")
            except Exception as e:
                logger.warning(f"停止任务时异常: {e}")

        # 3. Consul注销（最重要的一步）
        consul_success = False
        if self.consul_client:
            try:
                logger.info("🗑️  开始Consul注销流程...")

                # 给Consul注销足够的时间，确保能完成检查和清理
                consul_task = asyncio.create_task(
                    self.consul_client.deregister_service()
                )

                try:
                    consul_success = await asyncio.wait_for(consul_task, timeout=10.0)
                    if consul_success:
                        logger.info("✅ Consul注销成功")
                    else:
                        logger.error("❌ Consul注销失败")
                except asyncio.TimeoutError:
                    logger.error("⏰ Consul注销超时")
                    consul_task.cancel()
                except Exception as e:
                    logger.error(f"❌ Consul注销异常: {e}")
            except Exception as e:
                logger.error(f"💥 Consul注销过程异常: {e}")
        else:
            logger.info("ℹ️ 无Consul客户端，跳过注销")

        # 4. 停止gRPC服务器（在Consul注销之后）
        if self.grpc_server:
            try:
                logger.info("🛑 停止gRPC服务器...")
                await self.grpc_server.stop(grace=3.0)
                logger.info("✅ gRPC服务器已停止")
            except Exception as e:
                logger.error(f"❌ 停止gRPC服务器失败: {e}")

        # 5. 断开QQBot连接（最后一步）
        if self.sender:
            try:
                logger.info("🔌 断开QQBot连接...")
                await self.sender.disconnect()
                logger.info("✅ QQBot连接已断开")
            except Exception as e:
                logger.error(f"❌ 断开QQBot连接失败: {e}")

        logger.info(f"👋 QQBot微服务已停止 (Consul: {'已清理' if consul_success else '可能未清理'})")

    async def _setup_logging(self, log_config):
        """设置日志"""
        import sys

        logger.remove()

        if log_config.mode in ["console", "both"]:
            logger.add(
                sys.stdout,
                format="<green>{time:YYYY-MM-DD HH:mm:ss}</green> | "
                       "<level>{level: <8}</level> | "
                       "<cyan>{name}</cyan>:<cyan>{function}</cyan>:<cyan>{line}</cyan> - "
                       "<level>{message}</level>",
                level=log_config.level.upper(),
                colorize=True
            )

        if log_config.mode in ["file", "both"] and log_config.file_path:
            log_file = Path(log_config.file_path)
            log_file.parent.mkdir(parents=True, exist_ok=True)

            logger.add(
                str(log_file),
                format="{time:YYYY-MM-DD HH:mm:ss} | {level: <8} | "
                       "{name}:{function}:{line} - {message}",
                level=log_config.level.upper(),
                rotation="1 day",
                retention="7 days",
                encoding=log_config.encoding
            )

    async def _print_config(self, cfg):
        """打印配置"""
        logger.info("=" * 50)
        logger.info("📋 服务配置:")
        logger.info(f"   服务名称: {cfg.server.name}")
        logger.info(f"   监听地址: {cfg.server.listen_on}")
        logger.info(f"   运行模式: {cfg.server.mode}")
        logger.info(f"   最大工作线程: {cfg.server.max_workers}")

        if cfg.consul.host:
            logger.info(f"   Consul 地址: {cfg.consul.host}")

        logger.info(f"   日志级别: {cfg.log.level}")
        logger.info("=" * 50)
