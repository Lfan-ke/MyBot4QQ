import asyncio
import aiohttp

from common import (
    ConfigLoader, ConsulKVClient, PulsarService, KVServiceMeta,
)
from logger import logger
from service.qqbot import (
    QQMessage,
    qqbot_field_description,
)

config = ConfigLoader()


def build_url(base_url: str, endpoint: str) -> str:
    """
    构建正确的URL，确保只有一个斜杠
    """
    clean_base = base_url.rstrip("/")

    if not endpoint.startswith("/"):
        endpoint = f"/{endpoint}"

    return f"{clean_base}{endpoint}"


async def send_napcat_request(endpoint: str, params: dict[str, ...]) -> dict[str, ...]:
    """
    发送NapCat REST API请求 - 简单直接的异步请求
    """
    napcat_config = config.config.NapCat
    url = build_url(napcat_config.base_url, endpoint)

    try:
        timeout = aiohttp.ClientTimeout(total=napcat_config.TimeOut)
        headers = {
            "Authorization": f"Bearer {napcat_config.Token}",
            "Content-Type": "application/json",
            "User-Agent": "EchoWing/1.0"
        }

        await logger.debug(f"📤 发送请求到: {url}")

        async with aiohttp.ClientSession(timeout=timeout, headers=headers) as session:
            async with session.post(url, json=params) as response:
                if response.status == 200:
                    result = await response.json()
                    await logger.debug(f"📥 收到成功响应: {result.get('status', 'unknown')}")
                    return result
                else:
                    text = await response.text()
                    error_msg = f"HTTP {response.status}"
                    await logger.error(f"❌ 请求失败 {url}: {error_msg} - {text[:100]}")
                    return {
                        "status": "failed",
                        "error": error_msg,
                        "wording": text[:100] if text else ""
                    }

    except asyncio.TimeoutError:
        await logger.error(f"⏰ 请求超时: {url}")
        return {"status": "failed", "error": "请求超时"}
    except aiohttp.ClientError as e:
        await logger.error(f"🌐 网络错误 {url}: {e}")
        return {"status": "failed", "error": f"网络错误: {str(e)}"}
    except Exception as e:
        await logger.error(f"💥 请求异常 {url}: {e}")
        return {"status": "failed", "error": f"请求异常: {str(e)}"}


async def send_qq_message(qq_msg: QQMessage) -> bool:
    """
    发送QQ消息 - 简单的异步请求，不重试
    """
    try:
        # 获取标准化消息
        message_list = qq_msg.to_message()

        # 根据目标类型选择端点和参数
        if qq_msg.target_type.lower() == "user":
            endpoint = "send_private_msg"
            params = {
                "user_id": qq_msg.target_id,
                "message": message_list
            }
        elif qq_msg.target_type.lower() == "group":
            endpoint = "send_group_msg"
            params = {
                "group_id": qq_msg.target_id,
                "message": message_list
            }
        else:
            await logger.error(f"❌ 不支持的目标类型: {qq_msg.target_type}")
            return False

        # 发送请求
        result = await send_napcat_request(endpoint, params)

        # 处理结果
        if result.get("status") == "ok":
            # 记录成功日志
            metadata_summary = ""
            if qq_msg.metadata:
                special = []
                for key in ('user_id', 'app_id', 'function'):
                    if key in qq_msg.metadata:
                        special.append(f"{key}:{qq_msg.metadata[key]}")
                if special:
                    metadata_summary = f" [{', '.join(special)}]"

            await logger.info(f"✅ QQ消息发送成功: {qq_msg.target_type} {qq_msg.target_id}{metadata_summary}")
            return True
        else:
            # 记录失败日志
            error_msg = result.get("error", "未知错误")
            error_wording = result.get("wording", "")
            error_info = f"{error_msg}" + (f" ({error_wording})" if error_wording else "")

            await logger.error(f"❌ QQ消息发送失败 {qq_msg.target_type} {qq_msg.target_id}: {error_info}")
            return False

    except Exception as e:
        await logger.error(f"💥 发送QQ消息异常 {qq_msg.target_type} {qq_msg.target_id}: {e}")
        return False


async def qqbot_handler(payload: dict[str, ...]) -> bool:
    """
    QQBot服务处理器 - 简单的异步请求处理
    """
    try:
        # 解析消息
        qq_msg = QQMessage.from_dict(payload)

        # 发送消息
        success = await send_qq_message(qq_msg)

        return success

    except Exception as e:
        await logger.error(f"💥 [qqbot] 处理异常: {e}")
        return False


async def main():
    logger.set_app_name("EchoWing QQBot Service")

    # 创建Pulsar服务
    qqbot_service = PulsarService(
        service_name=config.config.Name,
        pulsar_url=config.config.Pulsar.Url,
        main_topic=config.main_topic(config.config.Name),
        dlq_topic=config.dlq_topic,
    )

    # 启动服务
    await qqbot_service.start(
        message_handler=qqbot_handler,
        max_redelivery_count=3
    )

    # Consul 注册
    consul = ConsulKVClient(
        host=config.config.Consul.Host,
        port=config.config.Consul.Port,
        token=config.config.Consul.Token,
        scheme=config.config.Consul.Scheme,
        kv_base_path=config.config.Consul.Base,
    )

    qqbot_schema = KVServiceMeta(
        ServerName=config.config.Name,
        ServerDesc="EchoWing QQ机器人消息服务",
        ServerIcon=None,
        ServerPath=config.main_topic(config.config.Name),
        ServerData={"fields": {
            **qqbot_field_description
        }}
    )

    await consul.register_kv(config.config.Name, qqbot_schema.to_dict())

    await logger.info(f"✅ 已注册 KV 到 Consul")
    await logger.info("🎯 QQBot服务已启动 (REST API 异步模式)")
    await logger.info(f"📡 NapCat API地址: {config.config.NapCat.Http}")
    await logger.info(f"⏱️ 请求超时设置: {config.config.NapCat.TimeOut}秒")
    await logger.info("🔄 重试逻辑由消息队列处理")
    await logger.info("🤖 服务监听中...")

    try:
        await asyncio.gather(qqbot_service.task)
    except asyncio.CancelledError:
        await logger.info("🛑 服务被终止")
    except Exception as e:
        await logger.error(f"💥 主程序异常: {e}")
    finally:
        # 清理资源
        await qqbot_service.stop()

        await consul.deregister_kv(config.config.Name)

        await logger.info("🚮 已注销 KV 从 Consul")
        await logger.info("✅ 服务已清理完成")


if __name__ == "__main__":
    asyncio.run(main())
