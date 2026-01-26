import yaml
from dataclasses import dataclass, field, asdict
from logger import logger


@dataclass
class PulsarConfig:
    Main: str = "persistent://echo-wing/main"
    Dlq: str = "persistent://echo-wing/dlq/all"
    Url: str = "pulsar://localhost:6650"

    def to_dict(self) -> dict[str, ...]:
        return asdict(self)

    @property
    def main_topic(self) -> str:
        return self.Main

    @property
    def dlq_topic(self) -> str:
        return self.Dlq


@dataclass
class ConsulConfig:
    Host: str = "localhost"
    Port: int = 8500
    Base: str = "echo_wing/"
    Token: str = ""
    Scheme: str = "http"

    @property
    def address(self) -> str:
        return f"{self.Scheme}://{self.Host}:{self.Port}"

    def to_dict(self) -> dict[str, ...]:
        return asdict(self)


@dataclass
class NapCatConfig:
    """NapCat REST API 配置"""
    Http: str = "http://localhost:3001"
    Token: str = "Qq360123."
    TimeOut: int = 10  # 保持10秒超时

    @property
    def base_url(self) -> str:
        """获取基础URL"""
        return self.Http.rstrip("/")

    def to_dict(self) -> dict[str, ...]:
        return asdict(self)


@dataclass
class AppConfig:
    Name: str
    Mode: str
    Pulsar: PulsarConfig = field(default_factory=PulsarConfig)
    Consul: ConsulConfig = field(default_factory=ConsulConfig)
    NapCat: NapCatConfig = field(default_factory=NapCatConfig)

    def to_dict(self) -> dict[str, ...]:
        data = asdict(self)
        data["Mode"] = self.Mode
        data["Pulsar"] = self.Pulsar.to_dict()
        data["Consul"] = self.Consul.to_dict()
        data["NapCat"] = self.NapCat.to_dict()
        return data


yaml_config: AppConfig | None = None


class ConfigLoader:
    def __init__(self, config_path: str = "config.yaml"):
        self.config_path = config_path
        global yaml_config
        if yaml_config is None:
            yaml_config = self.__load_config()
        self.config = yaml_config

    def main_topic(self, name: str) -> str:
        """根据服务名称构建完整的主topic路径"""
        return f"{self.config.Pulsar.main_topic}/{name}"

    @property
    def dlq_topic(self) -> str:
        return self.config.Pulsar.dlq_topic

    def __load_config(self) -> AppConfig:
        try:
            with open(self.config_path, 'r', encoding='utf-8') as f:
                yaml_data = yaml.safe_load(f)

            if not yaml_data:
                raise ValueError("配置文件为空")

            return self.__parse_config(yaml_data)

        except FileNotFoundError:
            raise FileNotFoundError(f"配置文件不存在: {self.config_path}")
        except yaml.YAMLError as e:
            raise ValueError(f"YAML 解析错误: {e}")
        except Exception as e:
            raise RuntimeError(f"加载配置失败: {e}")

    @staticmethod
    def __parse_config(yaml_data: dict[str, ...]) -> AppConfig:
        pulsar_data = yaml_data.get("Pulsar", {})
        pulsar_config = PulsarConfig(
            Main=pulsar_data.get("Main", "persistent://echo-wing/main"),
            Dlq=pulsar_data.get("Dlq", "persistent://echo-wing/dlq/all"),
            Url=pulsar_data.get("Url", "pulsar://localhost:6650")
        )

        consul_data = yaml_data.get("Consul", {})
        consul_config = ConsulConfig(
            Host=consul_data.get("Host", "localhost"),
            Port=consul_data.get("Port", 8500),
            Base=consul_data.get("Base", "echo_wing/"),
            Token=consul_data.get("Token", ""),
            Scheme=consul_data.get("Scheme", "http"),
        )

        napcat_data = yaml_data.get("NatCat", {}) or yaml_data.get("NapCat", {})
        napcat_config = NapCatConfig(
            Http=napcat_data.get("Http", "http://localhost:3001"),
            Token=napcat_data.get("Token", "Qq360123."),
            TimeOut=napcat_data.get("Timeout", napcat_data.get("TimeOut", 10))
        )

        configs = AppConfig(
            Name=yaml_data.get("Name", "qqbot"),
            Mode=yaml_data.get("Mode", "dev"),
            Pulsar=pulsar_config,
            Consul=consul_config,
            NapCat=napcat_config,
        )

        logger.info_sync(f"✅ QQBot配置加载成功: {configs.Name} [{configs.Mode}]")
        logger.info_sync(f"📡 NapCat API地址: {configs.NapCat.Http}")

        return configs
