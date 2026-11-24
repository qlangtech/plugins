## titleTemplate / messageTemplate

**模板语法**

使用 `${paramName}` 格式引用 Action 参数值：

**示例**：

假设 Action 参数为：
```json
{
  "issueName": "数据缺失",
  "tableName": "user_orders",
  "affectedRows": 1250,
  "recipient": "admin@example.com"
}
```

标题模板：
```
数据质量问题: ${issueName}
```

消息模板：
```
表 ${tableName} 发现数据质量问题。
问题类型: ${issueName}
影响行数: ${affectedRows}

请及时处理。
```

渲染结果：
```
标题: 数据质量问题: 数据缺失

消息:
表 user_orders 发现数据质量问题。
问题类型: 数据缺失
影响行数: 1250

请及时处理。
```

**占位符替换规则**：
- 参数名大小写敏感
- 如果参数不存在，占位符保持原样
- 参数值会调用 `toString()` 方法转换为字符串

## notifyChannel

**通知渠道**

渠道类型与收件人地址强相关，因此收敛为多态插件 `NotifyChannel`，由具体渠道子类自带收件人字段与格式校验：

| 渠道插件 | 说明 | 收件人字段 | 校验规则 |
|---|---|---|---|
| `EmailNotifyChannel`（邮件） | 邮件通知 | `emailAddress` 邮箱地址（如 `user@example.com`） | 非空 + Email 格式 |
| `SmsNotifyChannel`（短信） | 短信通知 | `phoneNumber` 手机号码（如 `13800138000`） | 非空 + 手机号格式 |
| `WebhookNotifyChannel`（Webhook） | Webhook 回调 | `webhookUrl` Webhook URL | 非空 + URL 格式 |
| `ImNotifyChannel`（即时消息） | 即时消息（如钉钉、企业微信） | `imTarget` 用户 ID 或群组 ID | 非空（各 IM 平台 ID 规则不统一，无格式校验） |

## priority

**优先级**

控制通知的紧急程度：
- **low**: 低优先级，延迟发送
- **normal**（默认）: 普通优先级，正常发送
- **high**: 高优先级，立即发送
- **urgent**: 紧急，多渠道推送

高优先级通知可能会：
- 更快送达
- 支持重试机制
- 在多个渠道同时推送
