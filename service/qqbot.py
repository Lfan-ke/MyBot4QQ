import json
from dataclasses import dataclass, field


def _normalize_content(content_item) -> dict[str, ...]:
    """标准化消息内容项，将字符串结构化为 json - type text"""
    if isinstance(content_item, str):
        # 字符串转换为 text 类型
        return {"type": "text", "data": {"text": content_item}}
    elif isinstance(content_item, dict):
        # 已经是字典格式，确保结构正确
        if "type" not in content_item:
            # 如果没有 type，假设是 text
            if "text" in content_item:
                return {"type": "text", "data": {"text": content_item["text"]}}
            else:
                # 尝试转换为 text
                return {"type": "text", "data": {"text": str(content_item)}}
        elif "data" not in content_item:
            # 有 type 但没有 data，添加空的 data
            return {"type": content_item["type"], "data": {}}
        else:
            # 格式正确
            return content_item
    else:
        # 其他类型转换为字符串
        return {"type": "text", "data": {"text": str(content_item)}}


@dataclass
class QQMessage:
    """QQ消息数据结构"""
    target_id: str
    target_type: str
    content: list[str | dict]
    metadata: dict[str, ...] = field(default_factory=dict)

    @classmethod
    def from_dict(cls, json_data: dict[str, ...]) -> 'QQMessage':
        """从字典创建消息"""
        data = json_data.copy()
        return cls(**{k: data[k] for k in data if k in cls.__annotations__})

    def to_message(self) -> list[dict]:
        """
        将消息内容转换为标准化的消息格式列表
        自动转换字符串为 {"type": "text", "data": {"text": "xxx"}} 格式
        """
        # 标准化消息内容
        normalized_content = []
        for item in self.content:
            normalized_item = _normalize_content(item)
            normalized_content.append(normalized_item)

        # 添加metadata到消息末尾
        content_with_metadata = self.__add_metadata_to_content(normalized_content)

        return content_with_metadata

    def __add_metadata_to_content(self, original_content: list[dict]) -> list[dict]:
        """将metadata格式化为文本并添加到消息末尾，私有方法使用两个下划线"""
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

        metadata_texts = []

        if formatted_lines:
            metadata_texts.append(f"| {' | '.join(formatted_lines)} |")

        if other_fields:
            try:
                other_json = json.dumps(other_fields, ensure_ascii=False, indent=2)
                metadata_texts.append(f"其他元数据:\n{other_json}")
            except Exception as _:
                metadata_texts.append(f"其他元数据: {str(other_fields)}")

        if metadata_texts:
            metadata_text = "\n\n".join(metadata_texts)

            # 将metadata作为文本元素添加到content末尾
            if original_content:
                last_item = original_content[-1]
                if last_item.get("type") == "text":
                    # 合并到最后一个文本元素
                    current_text = last_item.get("data", {}).get("text", "")
                    if current_text:
                        last_item["data"]["text"] = current_text + "\n\n" + metadata_text
                    else:
                        last_item["data"]["text"] = metadata_text
                else:
                    # 添加新的文本元素
                    original_content.append({
                        "type": "text",
                        "data": {"text": metadata_text}
                    })
            else:
                # 如果原本没有内容，直接添加metadata作为消息
                original_content.append({
                    "type": "text",
                    "data": {"text": metadata_text}
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
        "description": "可选元数据，比如app和user，用于记录",
        "required": False,
        "default": {},
    }
}
