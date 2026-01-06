"""
QQBot微服务测试客户端
"""
import asyncio
import json
import sys
from pathlib import Path
from typing import Optional, Tuple
import consul as consul_lib
import grpc
from loguru import logger

# 添加项目根目录到 Python 路径
sys.path.insert(0, str(Path(__file__).parent.parent))

from src.qqbot import qqbot_pb2, qqbot_pb2_grpc


class ConsulServiceDiscovery:
    """Consul服务发现"""

    def __init__(self, consul_host: str = "localhost:8500"):
        # 解析主机和端口
        if ":" in consul_host:
            host_str, port_str = consul_host.split(":", 1)
            port = int(port_str)
        else:
            host_str = consul_host
            port = 8500

        self.client = consul_lib.Consul(
            host=host_str,
            port=port,
            scheme="http",
            verify=False
        )
        self.consul_host = consul_host

    async def discover_service(self, service_name: str) -> Optional[Tuple[str, int]]:
        """
        从Consul发现服务

        Args:
            service_name: 服务名称

        Returns:
            (host, port) 元组，如果未找到则返回 None
        """
        try:
            logger.info(f"🔍 在 Consul {self.consul_host} 中查找服务: {service_name}")

            # 获取健康的服务实例
            index, nodes = self.client.health.service(
                service=service_name,
                passing=True
            )

            if not nodes:
                logger.warning(f"⚠️  未找到健康的服务实例: {service_name}")
                return None

            # 选择第一个健康的实例
            node = nodes[0]
            service_info = node.get('Service', {})

            address = service_info.get('Address', '')
            port = service_info.get('Port', 0)

            # 如果服务地址是空字符串，使用节点的地址
            if not address:
                address = node.get('Node', {}).get('Address', '')

            logger.info(f"✅ 发现服务: {service_name} -> {address}:{port}")

            return address, port

        except Exception as e:
            logger.error(f"❌ 服务发现失败: {e}")
            return None


class QQBotClient:
    """基于Consul发现的QQBot客户端"""

    def __init__(self, consul_host: str = "localhost:8500", service_name: str = "ew.qbot"):
        self.consul_host = consul_host
        self.service_name = service_name
        self.service_discovery = ConsulServiceDiscovery(consul_host)
        self.channel = None
        self.stub = None

    async def connect_via_consul(self) -> bool:
        """
        通过Consul发现并连接到服务

        Returns:
            是否连接成功
        """
        try:
            # 1. 从Consul发现服务
            service_info = await self.service_discovery.discover_service(self.service_name)

            if not service_info:
                logger.error(f"❌ 无法在Consul中找到服务: {self.service_name}")
                return False

            address, port = service_info
            target = f"{address}:{port}"

            # 2. 连接到gRPC服务
            logger.info(f"🔗 连接到 gRPC 服务: {target}")

            self.channel = grpc.aio.insecure_channel(target)
            self.stub = qqbot_pb2_grpc.QQBotServiceStub(self.channel)

            # 3. 测试连接
            try:
                response = await asyncio.wait_for(
                    self.stub.HealthCheck(qqbot_pb2.HealthCheckRequest()),
                    timeout=5.0
                )

                if response.status == 200:
                    logger.info(f"✅ 连接成功: {response.message}")
                    return True
                else:
                    logger.error(f"❌ 服务不健康: {response.message}")
                    return False

            except asyncio.TimeoutError:
                logger.error(f"⏰ 连接超时: {target}")
                return False

        except Exception as e:
            logger.error(f"💥 通过Consul连接失败: {e}")
            return False

    async def send_message(self, target_id: str, target_type: str, content_data: dict,
                          sender_id: str = "test_client") -> Optional[dict]:
        """
        发送QQ消息

        Args:
            target_id: 目标ID（QQ号或群号）
            target_type: 目标类型: "user" 或 "group"
            content_data: 消息内容数据
            sender_id: 发送者ID

        Returns:
            发送结果字典
        """
        if not self.stub:
            logger.error("❌ 未连接到服务")
            return None

        try:
            # 构建消息内容
            content = qqbot_pb2.MessageContent()

            # 根据内容类型设置字段
            if isinstance(content_data, str):
                content.text = content_data
            elif isinstance(content_data, dict):
                if content_data.get("type") == "MessageArray":
                    # 构建消息段列表
                    segments = []
                    for segment_data in content_data.get("data", []):
                        segment = qqbot_pb2.MessageSegment()
                        if isinstance(segment_data, dict):
                            segment.type = segment_data.get("type", "")
                            if isinstance(segment_data.get("data"), dict):
                                segment.data = json.dumps(segment_data["data"], ensure_ascii=False)
                            elif segment_data.get("data") is not None:
                                segment.data = json.dumps({"value": segment_data["data"]}, ensure_ascii=False)
                        elif isinstance(segment_data, str):
                            segment.type = "Text"
                            segment.data = json.dumps({"text": segment_data}, ensure_ascii=False)
                        segments.append(segment)

                    content.segments.extend(segments)
                else:
                    # 单个消息段
                    content.segment.type = content_data.get("type", "")
                    data = content_data.get("data", {})
                    if isinstance(data, dict):
                        content.segment.data = json.dumps(data, ensure_ascii=False)
                    elif data is not None:
                        content.segment.data = json.dumps({"value": data}, ensure_ascii=False)

            # 构建请求
            request = qqbot_pb2.SendMessageRequest(
                target_id=str(target_id),
                target_type=target_type,
                content=content,
                sender_id=sender_id,
                metadata={
                    "client": "test_client",
                    "consul_host": self.consul_host
                }
            )

            logger.info(f"📨 发送消息到: {target_type} {target_id}")

            # 发送请求
            response = await self.stub.SendMessage(request)

            result = {
                "success": response.status == 200,
                "status_code": response.status,
                "message": response.message
            }

            if response.data:
                try:
                    data = json.loads(response.data)
                    result.update(data)
                except:
                    result['raw_data'] = response.data

            if result['success']:
                logger.info(f"✅ 消息发送成功!")
            else:
                logger.error(f"❌ 消息发送失败: {result['message']}")

            return result

        except Exception as e:
            logger.error(f"💥 发送消息失败: {e}")
            return None

    async def health_check(self) -> Optional[dict]:
        """健康检查"""
        if not self.stub:
            logger.error("❌ 未连接到服务")
            return None

        try:
            response = await self.stub.HealthCheck(qqbot_pb2.HealthCheckRequest())

            result = {
                "status_code": response.status,
                "message": response.message
            }

            if response.data:
                try:
                    data = json.loads(response.data)
                    result.update(data)
                    logger.info(f"📊 健康状态: {response.message}")
                except:
                    logger.info(f"📋 原始数据: {response.data[:200]}...")

            return result

        except Exception as e:
            logger.error(f"❌ 健康检查失败: {e}")
            return None

    async def close(self):
        """关闭连接"""
        if self.channel:
            await self.channel.close()
            logger.info("🔌 连接已关闭")


async def main():
    """主测试函数"""
    import argparse

    parser = argparse.ArgumentParser(description="QQBot微服务测试客户端")
    parser.add_argument("--consul", default="dc-a588.local:8500", help="Consul服务器地址")
    parser.add_argument("--service", default="ew.qbot", help="服务名称")
    parser.add_argument("--target-id", default=3222087513, help="目标ID（QQ号或群号）")
    parser.add_argument("--target-type", choices=["user", "group"], default="user", help="目标类型")
    parser.add_argument("--content-type", choices=["text", "face", "image", "complex"], default="text",
                       help="消息类型")

    args = parser.parse_args()

    # 配置日志
    logger.remove()
    logger.add(
        sys.stdout,
        format="<green>{time:HH:mm:ss}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        level="INFO",
        colorize=True
    )

    client = QQBotClient(
        consul_host=args.consul,
        service_name=args.service
    )

    try:
        # 连接服务
        logger.info(f"🚀 通过 Consul 发现服务: {args.service}")
        connected = await client.connect_via_consul()

        if not connected:
            logger.error("❌ 无法连接到服务，测试终止")
            return

        print("\n" + "="*60)
        print("QQBot 微服务测试")
        print("="*60)

        # 健康检查
        logger.info("\n1. 🩺 健康检查...")
        health_result = await client.health_check()

        if health_result and health_result.get('status_code') == 200:
            logger.info("✅ 服务健康")
        else:
            logger.warning("⚠️  服务可能不健康")

        # 构建消息内容
        content_data = None

        if args.content_type == "text":
            content_data = f"【QQBot测试消息】\n时间: {asyncio.get_event_loop().time():.2f}\n这是一条测试消息。\n服务发现: {args.service} via {args.consul}"

        elif args.content_type == "face":
            content_data = {
                "type": "Face",
                "data": 14  # 表情ID
            }

        elif args.content_type == "image":
            content_data = {
                "type": "Image",
                "data": {
                    "url": "https://uploadstatic.mihoyo.com/contentweb/20210804/2021080419123130780.png",
                    "summary": "测试图片"
                }
            }

        elif args.content_type == "complex":
            content_data = {
                "type": "MessageArray",
                "data": [
                    "喵喵喵~这是复杂消息测试！",
                    {
                        "type": "Text",
                        "data": {"text": "文本消息段测试"}
                    },
                    {
                        "type": "Face",
                        "data": 14
                    }
                ]
            }

        # 发送消息
        logger.info(f"\n2. 📨 发送测试消息到: {args.target_type} {args.target_id}")

        send_result = await client.send_message(
            target_id=args.target_id,
            target_type=args.target_type,
            content_data=content_data,
            sender_id="test_client"
        )

        if send_result and send_result.get('success'):
            logger.info("🎉 测试消息发送成功!")
        else:
            logger.error("❌ 测试消息发送失败")

        print("\n" + "="*60)
        print("🎯 测试完成!")
        print("="*60)

        # 打印总结
        if send_result:
            print(f"\n📊 测试总结:")
            print(f"  目标: {args.target_type} {args.target_id}")
            print(f"  消息类型: {args.content_type}")
            print(f"  发送结果: {'✅ 成功' if send_result.get('success') else '❌ 失败'}")
            print(f"  响应消息: {send_result.get('message', 'N/A')}")
            print(f"  连接方式: Consul 发现 ({args.consul} -> {args.service})")

    except KeyboardInterrupt:
        logger.info("\n⌨️ 用户中断测试")
    except Exception as e:
        logger.error(f"💥 测试过程中发生错误: {e}")
        import traceback
        traceback.print_exc()
    finally:
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
