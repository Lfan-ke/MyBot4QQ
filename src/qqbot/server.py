"""
QQBot gRPC服务器实现
"""
import json
import time
import grpc
from concurrent import futures
from dataclasses import asdict
from loguru import logger

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.qqbot.sender import QQBotSender
from src.qqbot import qqbot_pb2, qqbot_pb2_grpc


class QQBotService(qqbot_pb2_grpc.QQBotServiceServicer):
    """
    QQBot gRPC服务实现
    """

    def __init__(self, sender: QQBotSender):
        self.sender = sender

    async def SendMessage(self, request, context):
        """发送单条消息"""
        logger.info(f"📨 发送QQ消息请求: {request.target_type} {request.target_id}")

        try:
            # 解析消息内容
            content = None

            if request.content.HasField("text"):
                content = request.content.text
            elif request.content.HasField("segment"):
                # 单个消息段
                segment_data = {
                    "type": request.content.segment.type,
                    "data": json.loads(request.content.segment.data) if request.content.segment.data else {}
                }
                content = segment_data
            elif request.content.segments:
                # 多个消息段
                segments_data = []
                for segment in request.content.segments:
                    segment_dict = {
                        "type": segment.type,
                        "data": json.loads(segment.data) if segment.data else {}
                    }
                    segments_data.append(segment_dict)

                content = {
                    "type": "MessageArray",
                    "data": segments_data
                }

            if not content:
                return qqbot_pb2.SendMessageResponse(
                    status=400,
                    message="消息内容不能为空",
                    data=json.dumps({"error": "Empty content"}, ensure_ascii=False)
                )

            # 构建元数据
            metadata = dict(request.metadata)
            if request.sender_id:
                metadata['sender_id'] = request.sender_id

            # 发送消息
            result = await self.sender.send_message(
                target_id=request.target_id,
                target_type=request.target_type,
                content=content,
                metadata=metadata
            )

            # 使用dataclass的asdict方法
            result_dict = asdict(result)

            # 构建响应
            status_code = 200 if result.success else 500

            return qqbot_pb2.SendMessageResponse(
                status=status_code,
                message=result.message,
                data=json.dumps(result_dict, ensure_ascii=False)
            )

        except Exception as e:
            logger.error(f"💥 处理发送消息请求失败: {e}")

            error_data = {
                "error": str(e),
                "timestamp": time.time(),
                "target_id": request.target_id,
                "target_type": request.target_type,
                "success": False
            }

            return qqbot_pb2.SendMessageResponse(
                status=500,
                message=f"内部服务器错误: {str(e)}",
                data=json.dumps(error_data, ensure_ascii=False)
            )

    async def SendBatchMessages(self, request, context):
        """批量发送消息"""
        logger.info(f"📦 批量发送QQ消息请求，数量: {len(request.target_ids)}")

        try:
            # 解析消息内容
            content = None

            if request.content.HasField("text"):
                content = request.content.text
            elif request.content.HasField("segment"):
                # 单个消息段
                segment_data = {
                    "type": request.content.segment.type,
                    "data": json.loads(request.content.segment.data) if request.content.segment.data else {}
                }
                content = segment_data
            elif request.content.segments:
                # 多个消息段
                segments_data = []
                for segment in request.content.segments:
                    segment_dict = {
                        "type": segment.type,
                        "data": json.loads(segment.data) if segment.data else {}
                    }
                    segments_data.append(segment_dict)

                content = {
                    "type": "MessageArray",
                    "data": segments_data
                }

            if not content:
                return qqbot_pb2.SendBatchMessagesResponse(
                    status=400,
                    message="消息内容不能为空",
                    data=json.dumps({"error": "Empty content"}, ensure_ascii=False)
                )

            # 构建元数据
            metadata = dict(request.metadata)
            if request.sender_id:
                metadata['sender_id'] = request.sender_id

            # 批量发送消息
            result = await self.sender.send_batch_messages(
                target_ids=list(request.target_ids),
                target_type=request.target_type,
                content=content,
                metadata=metadata
            )

            # 构建响应
            overall_success = result.get('success_count', 0) > 0
            status_code = 200 if overall_success else 500

            return qqbot_pb2.SendBatchMessagesResponse(
                status=status_code,
                message=f"批量发送完成，成功 {result.get('success_count', 0)} 条，失败 {result.get('failure_count', 0)} 条",
                data=json.dumps(result, ensure_ascii=False)
            )

        except Exception as e:
            logger.error(f"💥 处理批量发送请求失败: {e}")

            error_data = {
                "error": str(e),
                "timestamp": time.time(),
                "target_ids_count": len(request.target_ids),
                "target_type": request.target_type,
                "success": False
            }

            return qqbot_pb2.SendBatchMessagesResponse(
                status=500,
                message=f"内部服务器错误: {str(e)}",
                data=json.dumps(error_data, ensure_ascii=False)
            )

    async def HealthCheck(self, request, context):
        """健康检查"""
        try:
            # 检查QQBot状态
            health_status = await self.sender.health_check()

            # 使用dataclass的asdict方法
            health_dict = asdict(health_status)

            health_data = {
                "timestamp": time.time(),
                "service_ready": health_status.healthy,  # 直接访问属性
                "health_status": "healthy" if health_status.healthy else "unhealthy",
                "details": health_dict
            }

            status_code = 200 if health_status.healthy else 503

            return qqbot_pb2.HealthCheckResponse(
                status=status_code,
                message="服务健康" if health_status.healthy else "服务不健康",
                data=json.dumps(health_data, ensure_ascii=False)
            )

        except Exception as e:
            logger.error(f"💥 健康检查失败: {e}")

            error_data = {
                "timestamp": time.time(),
                "service_ready": False,
                "error": str(e),
                "details": "健康检查异常"
            }

            return qqbot_pb2.HealthCheckResponse(
                status=500,
                message=f"健康检查失败: {str(e)}",
                data=json.dumps(error_data, ensure_ascii=False)
            )


def create_server(sender: QQBotSender, max_workers: int = 10) -> grpc.aio.Server:
    """
    创建gRPC服务器

    Args:
        sender: QQ消息发送器
        max_workers: 最大工作线程数

    Returns:
        gRPC服务器实例
    """
    server = grpc.aio.server(
        futures.ThreadPoolExecutor(max_workers=max_workers)
    )

    # 添加服务
    qqbot_service = QQBotService(sender)
    qqbot_pb2_grpc.add_QQBotServiceServicer_to_server(qqbot_service, server)

    return server
