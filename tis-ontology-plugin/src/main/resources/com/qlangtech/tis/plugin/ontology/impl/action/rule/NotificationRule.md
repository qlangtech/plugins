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

## notificationType

**通知渠道类型**

支持的通知类型：
- **email**（默认）: 邮件通知
- **sms**: 短信通知
- **webhook**: Webhook 回调
- **im**: 即时消息（如钉钉、企业微信）

不同类型对 `recipient` 参数的要求：
- `email`: 邮箱地址（如 `user@example.com`）
- `sms`: 手机号码（如 `13800138000`）
- `webhook`: Webhook URL
- `im`: 用户 ID 或群组 ID

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
