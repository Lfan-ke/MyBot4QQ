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
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "title": "QQBot Message Schema",
  "description": "QQ机器人消息发送格式",
  "properties": {
    "target_id": {
      "type": "string",
      "description": "QQ号或群号",
      "pattern": "^\\d{5,15}$"
    },
    "target_type": {
      "type": "string",
      "description": "私聊(user)或群聊(group)",
      "enum": ["user", "group"]
    },
    "content": {
      "type": "array",
      "description": "消息内容，支持多种消息格式的数组",
      "minItems": 1,
      "items": {
        "oneOf": [
          {
            "type": "string",
            "description": "简写格式：纯文本消息，内部会自动转换为text类型"
          },
          {
            "type": "object",
            "properties": {
              "type": {
                "type": "string",
                "const": "text",
                "description": "文本消息"
              },
              "data": {
                "type": "object",
                "properties": {
                  "text": {
                    "type": "string",
                    "description": "文本内容，支持CQ码和转义字符"
                  }
                },
                "required": ["text"],
                "additionalProperties": False
              }
            },
            "required": ["type", "data"],
            "additionalProperties": False
          },
          {
            "type": "object",
            "properties": {
              "type": {
                "type": "string",
                "const": "at",
                "description": "@某人"
              },
              "data": {
                "type": "object",
                "properties": {
                  "qq": {
                    "oneOf": [
                      {
                        "type": "string",
                        "description": "QQ号，用于@特定用户",
                        "pattern": "^\\d{5,15}$"
                      },
                      {
                        "type": "string",
                        "const": "all",
                        "description": "特殊值，@全体成员"
                      }
                    ]
                  }
                },
                "required": ["qq"],
                "additionalProperties": False
              }
            },
            "required": ["type", "data"],
            "additionalProperties": False
          },
          {
            "type": "object",
            "properties": {
              "type": {
                "type": "string",
                "const": "image",
                "description": "图片消息"
              },
              "data": {
                "type": "object",
                "properties": {
                  "file": {
                    "type": "string",
                    "description": "图片文件路径/URL/base64://编码数据"
                  },
                  "url": {
                    "type": "string",
                    "description": "图片URL（可选，与file互斥）"
                  },
                  "cache": {
                    "type": "boolean",
                    "description": "是否缓存图片",
                    "default": True
                  }
                },
                "required": ["file"],
                "additionalProperties": False
              }
            },
            "required": ["type", "data"],
            "additionalProperties": False
          },
          {
            "type": "object",
            "properties": {
              "type": {
                "type": "string",
                "const": "face",
                "description": "QQ表情消息"
              },
              "data": {
                "type": "object",
                "properties": {
                  "id": {
                    "type": "integer",
                    "description": "表情ID，如14表示微笑",
                    "minimum": 0,
                    "maximum": 255
                  }
                },
                "required": ["id"],
                "additionalProperties": False
              }
            },
            "required": ["type", "data"],
            "additionalProperties": False
          },
          {
            "type": "object",
            "properties": {
              "type": {
                "type": "string",
                "const": "record",
                "description": "语音消息"
              },
              "data": {
                "type": "object",
                "properties": {
                  "file": {
                    "type": "string",
                    "description": "语音文件路径/URL，支持amr、silk、mp3格式"
                  },
                  "magic": {
                    "type": "boolean",
                    "description": "是否变声",
                    "default": False
                  },
                  "cache": {
                    "type": "boolean",
                    "description": "是否缓存",
                    "default": True
                  }
                },
                "required": ["file"],
                "additionalProperties": False
              }
            },
            "required": ["type", "data"],
            "additionalProperties": False
          },
          {
            "type": "object",
            "properties": {
              "type": {
                "type": "string",
                "const": "video",
                "description": "视频消息"
              },
              "data": {
                "type": "object",
                "properties": {
                  "file": {
                    "type": "string",
                    "description": "视频文件路径/URL，支持mp4格式"
                  },
                  "url": {
                    "type": "string",
                    "description": "视频URL（可选）"
                  },
                  "cover": {
                    "type": "string",
                    "description": "封面图片URL（可选）"
                  },
                  "cache": {
                    "type": "boolean",
                    "description": "是否缓存",
                    "default": True
                  }
                },
                "required": ["file"],
                "additionalProperties": False
              }
            },
            "required": ["type", "data"],
            "additionalProperties": False
          },
          {
            "type": "object",
            "properties": {
              "type": {
                "type": "string",
                "const": "file",
                "description": "文件消息"
              },
              "data": {
                "type": "object",
                "properties": {
                  "file": {
                    "type": "string",
                    "description": "文件路径，服务器本地或网络文件均可"
                  },
                  "name": {
                    "type": "string",
                    "description": "文件名（可选）"
                  }
                },
                "required": ["file"],
                "additionalProperties": False
              }
            },
            "required": ["type", "data"],
            "additionalProperties": False
          },
          {
            "type": "object",
            "properties": {
              "type": {
                "type": "string",
                "const": "reply",
                "description": "回复消息"
              },
              "data": {
                "type": "object",
                "properties": {
                  "id": {
                    "type": "integer",
                    "description": "回复消息的message_id"
                  },
                  "seq": {
                    "type": "integer",
                    "description": "回复消息的序列号，与id二选一"
                  }
                },
                "oneOf": [
                  {
                    "required": ["id"]
                  },
                  {
                    "required": ["seq"]
                  }
                ],
                "additionalProperties": False
              }
            },
            "required": ["type", "data"],
            "additionalProperties": False
          },
          {
            "type": "object",
            "properties": {
              "type": {
                "type": "string",
                "const": "json",
                "description": "JSON卡片消息"
              },
              "data": {
                "type": "object",
                "properties": {
                  "data": {
                    "type": "string",
                    "description": "JSON格式的卡片数据字符串"
                  }
                },
                "required": ["data"],
                "additionalProperties": False
              }
            },
            "required": ["type", "data"],
            "additionalProperties": False
          },
          {
            "type": "object",
            "properties": {
              "type": {
                "type": "string",
                "const": "dice",
                "description": "掷骰子（随机生成1-6点数）"
              }
            },
            "required": ["type"],
            "additionalProperties": False
          },
          {
            "type": "object",
            "properties": {
              "type": {
                "type": "string",
                "const": "rps",
                "description": "猜拳（随机生成石头剪刀布）"
              }
            },
            "required": ["type"],
            "additionalProperties": False
          },
          {
            "type": "object",
            "properties": {
              "type": {
                "type": "string",
                "const": "music",
                "description": "音乐分享消息"
              },
              "data": {
                "type": "object",
                "oneOf": [
                  {
                    "type": "object",
                    "description": "平台音乐（QQ音乐、网易云等）",
                    "properties": {
                      "type": {
                        "type": "string",
                        "enum": ["qq", "163", "xm"],
                        "description": "音乐平台：qq(QQ音乐)、163(网易云)、xm(虾米)"
                      },
                      "id": {
                        "type": "string",
                        "description": "平台歌曲ID"
                      },
                      "singer": {
                        "type": "string",
                        "description": "歌手（可选）"
                      },
                      "title": {
                        "type": "string",
                        "description": "歌曲标题（可选）"
                      }
                    },
                    "required": ["type", "id"],
                    "additionalProperties": False
                  },
                  {
                    "type": "object",
                    "description": "自定义音乐",
                    "properties": {
                      "type": {
                        "type": "string",
                        "const": "custom"
                      },
                      "url": {
                        "type": "string",
                        "format": "uri",
                        "description": "歌曲跳转链接"
                      },
                      "audio": {
                        "type": "string",
                        "format": "uri",
                        "description": "音频流链接"
                      },
                      "title": {
                        "type": "string",
                        "description": "歌曲标题"
                      },
                      "image": {
                        "type": "string",
                        "format": "uri",
                        "description": "封面图片链接（可选）"
                      },
                      "singer": {
                        "type": "string",
                        "description": "歌手（可选）"
                      }
                    },
                    "required": ["type", "url", "audio", "title"],
                    "additionalProperties": False
                  }
                ]
              }
            },
            "required": ["type", "data"],
            "additionalProperties": False
          },
          {
            "type": "object",
            "description": "消息节点（用于合并转发）",
            "properties": {
              "type": {
                "type": "string",
                "const": "node"
              },
              "data": {
                "type": "object",
                "properties": {
                  "user_id": {
                    "type": "string",
                    "pattern": "^\\d{5,15}$",
                    "description": "发送者QQ号"
                  },
                  "nickname": {
                    "type": "string",
                    "description": "发送者昵称"
                  },
                  "content": {
                    "$ref": "#/properties/content",
                    "description": "节点内的消息内容，结构与外层content相同"
                  }
                },
                "required": ["user_id", "nickname", "content"],
                "additionalProperties": False
              }
            },
            "required": ["type", "data"],
            "additionalProperties": False
          }
        ]
      }
    },
    "metadata": {
      "type": "object",
      "description": "可选元数据，比如app和user，用于记录",
      "default": {},
      "additionalProperties": True
    }
  },
  "required": ["target_id", "target_type", "content"]
}
