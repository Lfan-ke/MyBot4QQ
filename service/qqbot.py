import json
from dataclasses import dataclass, field


@dataclass
class QQMessage:
    """QQ消息数据结构"""
    target_id: str
    target_type: str
    content: list[...]
    metadata: dict[str, ...] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, json_data: dict[str, ...]) -> 'QQMessage':
        """从字典创建消息"""
        data = json_data.copy()
        return cls(**{k: data[k] for k in data if k in cls.__annotations__})

    def to_ncatbot_params(self) -> dict[str, ...]:
        """
        将消息转换为ncatbot API参数

        content支持格式示例:
        1. 纯文本列表: ["hello", "world"]
        2. 消息元素混合: ["喵喵喵", {"type": "text", "data": {"text": "你好"}}]
        3. 完整消息元素: [{"type": "text", "data": {"text": "hi"}}, {"type": "face", "data": {"id": "14"}}]
        4. 简单文本: ["一条消息"]
        """
        # 处理metadata，将其格式化为文本并添加到content末尾
        content_with_metadata = self.__add_metadata_to_content(self.content.copy())

        # ncatbot参数
        params = {
            "target_id": self.target_id,
            "rtf": content_with_metadata  # ncatbot的rtf参数直接接受消息数组
        }

        return params

    def __add_metadata_to_content(self, original_content: list[...]) -> list[...]:
        """
        将metadata格式化为文本并添加到消息末尾

        格式:
        特殊字段 (user_id, app_id, function) 用 ` · ` 分隔
        其他字段用JSON格式显示
        """
        if not self.metadata:
            return original_content

        # 处理特殊字段
        special_fields = ('user_id', 'app_id', 'function')
        formatted_lines = []

        for field_name in special_fields:
            if field_name in self.metadata:
                formatted_lines.append(f"{field_name}: {self.metadata[field_name]}")

        # 处理其他字段
        other_fields = {k: v for k, v in self.metadata.items() if k not in special_fields}

        # 构建metadata文本
        metadata_texts = []

        if formatted_lines:
            # 特殊字段用 ` · ` 分隔
            metadata_texts.append(f"| {' · '.join(formatted_lines)} |")

        if other_fields:
            # 其他字段用JSON格式
            try:
                # 尝试美化JSON
                other_json = json.dumps(other_fields, ensure_ascii=False, indent=2)
                metadata_texts.append(f"其他元数据:\n{other_json}")
            except Exception as _:
                metadata_texts.append(f"其他元数据: {str(other_fields)}")

        if metadata_texts:
            # 将metadata作为文本元素添加到content末尾
            # 先检查最后一个元素是否是文本，可以合并
            if (original_content and
                isinstance(original_content[-1], dict) and
                original_content[-1].get("type") == "text"):
                # 合并到最后一个文本元素
                last_text = original_content[-1]
                if "data" in last_text and "text" in last_text["data"]:
                    # 在现有文本后添加metadata
                    last_text["data"]["text"] += "\n\n" + "\n\n".join(metadata_texts)
                else:
                    # 添加新的文本元素
                    original_content.append({
                        "type": "text",
                        "data": {"text": "\n\n".join(metadata_texts)}
                    })
            else:
                # 添加新的文本元素
                # 先确保有分隔
                if original_content:
                    # 如果不是文本，需要添加分隔
                    original_content.append({
                        "type": "text",
                        "data": {"text": ""}
                    })

                # 添加metadata文本
                original_content.append({
                    "type": "text",
                    "data": {"text": "\n\n".join(metadata_texts)}
                })

        return original_content


qqbot_field_description = {
    "target_id": {
        "type": "str",
        "description": "QQ号或群号",
        "required": True,
        "pattern": r"^\d{5,15}$",
    },
    "target_type": {
        "type": "enum",
        "description": "私聊(user)或群聊(group)",
        "required": True,
        "enum": ["user", "group"],
    },
    "content": {
        "type": "list",
        "description": "消息内容，支持多种格式，详见ncatbot的ref文档，以后写描述...",  # 以后完善规范之后弄...
        "required": True,
    },
    "metadata": {
        "type": "dict",
        "description": "可选元数据，比如app和user，由于记录",
        "required": False,
        "default": {},
    }
}
