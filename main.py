import asyncio
from ncatbot.core import BotClient

from common import (
    ConfigLoader, ConsulKVClient, PulsarService, KVServiceMeta,
)
from logger import logger
from service.qqbot import (
    QQMessage,
    qqbot_field_description,
)

config = ConfigLoader()


class QQBotSender:
    """QQBot发送器（简化版）"""

    def __init__(self):
        self._api = None
        self._connected = False

    async def connect(self) -> bool:
        """连接到QQBot"""
        if self._connected and self._api:
            return True

        try:
            logger.info_sync("🔗 连接到QQBot...")

            try:
                bot_client = BotClient()
                self._api = bot_client.run_backend()

                test_result = self._api.get_login_info_sync()

                if test_result and hasattr(test_result, 'user_id'):
                    self._connected = True

                    user_id = getattr(test_result, 'user_id', '未知')
                    nickname = getattr(test_result, 'nickname', '未知')
                    logger.info_sync(f"✅ QQBot连接成功: {nickname}({user_id})")
                    return True
                else:
                    logger.error_sync("❌ QQBot连接测试失败: 无法获取登录信息")
                    return False

            except Exception as e:
                logger.error_sync(f"❌ 创建QQBot API连接失败: {e}")
                return False

        except Exception as e:
            logger.error_sync(f"💥 QQBot连接失败: {e}")
            return False

    async def send_message(self, qq_msg: QQMessage) -> bool:
        """发送QQ消息"""
        try:
            if not await self.connect():
                return False

            # ncatbot的rtf参数直接接受消息数组
            rtf_content = qq_msg.content

            # 根据target_type发送消息
            if qq_msg.target_type.lower() == "user":
                # 私聊消息
                result = self._api.post_private_msg_sync(
                    user_id=qq_msg.target_id,
                    rtf=rtf_content
                )
            elif qq_msg.target_type.lower() == "group":
                # 群聊消息
                result = self._api.post_group_msg_sync(
                    group_id=qq_msg.target_id,
                    rtf=rtf_content
                )
            else:
                logger.error_sync(f"❌ 不支持的目标类型: {qq_msg.target_type}")
                return False

            if result:
                logger.info_sync(f"✅ QQ消息发送成功: {qq_msg.target_type} {qq_msg.target_id}")
                return True
            else:
                logger.error_sync(f"❌ QQ消息发送失败: {result}")
                return False

        except Exception as e:
            logger.error_sync(f"💥 发送QQ消息异常: {e}")
            # 连接失效，重置状态
            self._connected = False
            self._api = None
            return False


async def qqbot_handler(payload: dict[str, ...]) -> bool:
    """QQBot服务处理器"""
    try:
        # 解析消息
        qq_msg = QQMessage.from_dict(payload)

        # 创建QQBot发送器并发送消息
        qqbot_sender = QQBotSender()
        success = await qqbot_sender.send_message(qq_msg)

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
    )

    consul = ConsulKVClient(
        host=config.config.Consul.Host,
        port=config.config.Consul.Port,
        token=config.config.Consul.Token,
        scheme=config.config.Consul.Scheme,
        kv_base_path=config.config.Consul.Base,
    )

    # 注册服务到Consul
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
    await logger.info("🎯 QQBot服务已启动，配置了自动重试和死信队列")
    await logger.info("🤖 服务监听中...")

    try:
        await asyncio.gather(qqbot_service.task)
    except asyncio.CancelledError:
        await logger.info("🛑 服务被终止")
    except Exception as e:
        await logger.error(f"💥 主程序异常: {e}")
    finally:
        await qqbot_service.stop()

        await consul.deregister_kv(config.config.Name)

        await logger.info("🚮 已注销 KV 从 Consul")


if __name__ == "__main__":
    asyncio.run(main())
