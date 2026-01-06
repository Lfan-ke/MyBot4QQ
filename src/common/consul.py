"""
Consul客户端模块
"""
import json
import time
import socket
import os
import asyncio
from typing import Optional
from dataclasses import dataclass, field, asdict
import consul as consul_lib
from loguru import logger


@dataclass
class KVServiceMeta:
    """KV存储的服务元信息"""
    ServerName: str
    ServerDesc: str = ""
    ServerData: dict = field(default_factory=dict)
    created_at: int = field(default_factory=lambda: int(time.time()))
    updated_at: int = field(default_factory=lambda: int(time.time()))


class ConsulClient:
    """
    Consul客户端
    """

    def __init__(
        self,
        host: str,
        token: str = "",
        scheme: str = "http",
        kv_base_path: str = "echo_wing/"
    ):
        # 解析主机和端口
        if ":" in host:
            host_str, port_str = host.split(":", 1)
            port = int(port_str)
        else:
            host_str = host
            port = 8500

        # 创建Consul客户端
        self.client = consul_lib.Consul(
            host=host_str,
            port=port,
            token=token if token else None,
            scheme=scheme,
            verify=False
        )

        self.kv_base_path = kv_base_path.rstrip("/") + "/"
        self.service_id: Optional[str] = None
        self.service_name: Optional[str] = None
        self.kv_path: Optional[str] = None
        self.registered: bool = False

    async def register_service(
        self,
        service_name: str,
        address: str,
        port: int,
        service_desc: str = "",
        server_data: Optional[dict] = None,
        meta: Optional[dict[str, str]] = None
    ) -> bool:
        """
        注册服务到Consul

        核心逻辑：如果KV不存在就创建KV，然后注册服务
        如果KV已存在就直接使用，然后注册服务
        不关心KV是谁创建的
        """
        if self.registered:
            logger.warning(f"⚠️ 服务 {service_name} 已注册，跳过重复注册")
            return True

        self.service_name = service_name
        self.kv_path = f"{self.kv_base_path}{service_name}"

        try:
            # 1. 检查KV是否存在，如果不存在就创建
            kv_exists = await self._check_kv_exists()
            if not kv_exists:
                logger.info(f"📁 KV不存在，创建KV: {self.kv_path}")
                await self._register_kv(service_desc, server_data)
            else:
                logger.info(f"📁 KV已存在，直接使用: {self.kv_path}")

            # 2. 生成唯一的服务ID
            hostname = socket.gethostname()
            pid = os.getpid()
            timestamp = int(time.time())
            self.service_id = f"{service_name}-{hostname}-{pid}-{port}-{timestamp}"

            # 3. 准备注册数据
            tags = ["qqbot", "notification", "grpc", "qq"]

            if meta is None:
                meta = {
                    "kv_path": self.kv_path,
                    "version": "1.0.0",
                    "host": hostname,
                    "pid": str(pid),
                    "started": str(timestamp),
                }
            else:
                meta["kv_path"] = self.kv_path

            # 4. 使用TCP检查
            check = {
                "TCP": f"{address}:{port}",
                "Interval": "10s",
                "Timeout": "5s",
                "DeregisterCriticalServiceAfter": "30s"
            }

            # 5. 注册服务
            self.client.agent.service.register(
                name=service_name,
                service_id=self.service_id,
                address=address,
                port=port,
                tags=tags,
                meta=meta,
                check=check
            )

            self.registered = True
            logger.info(f"✅ 服务 {service_name} 注册成功 (ID: {self.service_id})")
            return True

        except Exception as e:
            logger.error(f"❌ 服务注册失败: {e}")
            return False

    async def deregister_service(self) -> bool:
        """
        从Consul注销服务

        核心逻辑：
        1. 先注销自己的服务（如果已注册）
        2. 检查是否还有此类服务的其他实例
        3. 如果没有其他实例，清理KV
        """
        try:
            logger.info(f"🗑️  开始注销服务: {self.service_name or '未知'}")

            # 1. 先注销自己的服务（如果已注册）
            service_deregistered = True
            if self.registered and self.service_id:
                service_deregistered = await self._deregister_service_with_retry()
                if not service_deregistered:
                    logger.error("❌ 服务注销失败")
                    # 但继续检查KV状态，因为可能服务已不存在于Consul

            # 2. 检查是否还有其他实例
            if self.service_name:
                has_other_instances = await self._has_other_instances()

                # 3. 如果没有其他实例，清理KV
                if not has_other_instances:
                    logger.info("🔍 无其他活跃实例，清理KV...")
                    await self._delete_kv_if_last_instance()
                else:
                    active_count = await self._get_active_instance_count()
                    logger.info(f"📋 还有 {active_count} 个活跃实例，保留KV")
            else:
                logger.warning("⚠️ 服务名未知，跳过KV检查")

            # 4. 重置状态
            self._reset_state()

            logger.info("✅ Consul注销流程完成")
            return service_deregistered

        except Exception as e:
            logger.error(f"💥 服务注销过程异常: {e}")
            self._reset_state()  # 异常时也重置状态
            return False

    async def _deregister_service_with_retry(self) -> bool:
        """重试注销服务"""
        max_retries = 3

        for i in range(max_retries):
            try:
                if i > 0:
                    logger.info(f"🔄 服务注销重试 {i}/{max_retries}...")
                    await asyncio.sleep(i)  # 指数退避

                # 使用线程池执行同步的Consul操作
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(
                    None,
                    lambda: self.client.agent.service.deregister(self.service_id)
                )

                logger.info(f"✅ 服务注销成功: {self.service_name}")
                return True

            except Exception as e:
                logger.warning(f"⚠️ 服务注销失败 (尝试 {i+1}/{max_retries}): {e}")

        return False

    async def _has_other_instances(self) -> bool:
        """检查是否有其他活跃实例"""
        if not self.service_name:
            return False

        try:
            active_count = await self._get_active_instance_count()
            logger.info(f"🔍 检查其他实例: 发现 {active_count} 个活跃实例")
            return active_count > 0

        except Exception as e:
            logger.warning(f"⚠️ 检查其他实例失败: {e}")
            # 出错时保守处理，假设还有其他实例，避免误删KV
            return True

    async def _get_active_instance_count(self) -> int:
        """获取活跃实例数量（不包括自己）"""
        try:
            loop = asyncio.get_event_loop()

            # 使用线程池执行同步的Consul操作
            index, nodes = await loop.run_in_executor(
                None,
                lambda: self.client.health.service(
                    service=self.service_name,
                    passing=True
                )
            )

            if not nodes:
                return 0

            # 统计其他实例数量（排除自己）
            other_count = 0
            for node in nodes:
                service_info = node.get('Service', {})
                if service_info.get('ID') != self.service_id:
                    other_count += 1

            return other_count

        except Exception as e:
            logger.warning(f"⚠️ 获取实例数量失败: {e}")
            return 0

    async def _delete_kv_if_last_instance(self):
        """如果是最后一个实例，删除KV"""
        if not self.kv_path:
            logger.warning("⚠️ KV路径未知，无法删除")
            return

        max_retries = 3
        logger.info(f"🗑️  准备删除KV: {self.kv_path}")

        for i in range(max_retries):
            try:
                if i > 0:
                    logger.info(f"🔄 KV删除重试 {i}/{max_retries}...")
                    await asyncio.sleep(i)

                loop = asyncio.get_event_loop()

                # 先检查KV是否存在（双重确认）
                index, data = await loop.run_in_executor(
                    None,
                    lambda: self.client.kv.get(self.kv_path)
                )

                if data is None:
                    logger.info(f"ℹ️ KV已不存在: {self.kv_path}")
                    return

                # 删除KV
                success = await loop.run_in_executor(
                    None,
                    lambda: self.client.kv.delete(self.kv_path)
                )

                if success:
                    logger.info(f"✅ KV删除成功: {self.kv_path}")
                    return
                else:
                    logger.warning(f"⚠️ KV删除返回失败 (尝试 {i+1}/{max_retries})")

            except Exception as e:
                logger.warning(f"⚠️ KV删除异常 (尝试 {i+1}/{max_retries}): {e}")

        logger.warning(f"⚠️ KV删除失败: {self.kv_path}")

    async def _check_kv_exists(self) -> bool:
        """检查KV是否存在"""
        try:
            loop = asyncio.get_event_loop()
            index, data = await loop.run_in_executor(
                None,
                lambda: self.client.kv.get(self.kv_path)
            )
            exists = data is not None
            logger.info(f"📁 KV状态检查: {self.kv_path} - {'存在' if exists else '不存在'}")
            return exists
        except Exception as e:
            logger.warning(f"⚠️ 检查KV存在性失败: {e}")
            return False

    async def _register_kv(self, service_desc: str, server_data: Optional[dict]) -> bool:
        """注册KV"""
        try:
            # 准备KV元数据
            kv_meta = KVServiceMeta(
                ServerName=self.service_name,
                ServerDesc=service_desc,
                ServerData=server_data or {}
            )

            # 注册KV
            loop = asyncio.get_event_loop()
            data_str = json.dumps(asdict(kv_meta), ensure_ascii=False)
            success = await loop.run_in_executor(
                None,
                lambda: self.client.kv.put(self.kv_path, data_str)
            )

            if success:
                logger.info(f"✅ KV注册成功: {self.kv_path}")
            else:
                logger.warning(f"⚠️ KV注册失败: {self.kv_path}")

            return success

        except Exception as e:
            logger.error(f"❌ KV注册失败: {e}")
            return False

    def _reset_state(self):
        """重置状态"""
        self.registered = False
        self.service_id = None
        self.service_name = None
        self.kv_path = None
