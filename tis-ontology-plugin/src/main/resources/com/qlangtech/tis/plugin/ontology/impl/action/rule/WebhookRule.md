## headers

**HTTP 请求头配置**

每行配置一个请求头，格式：`Header-Name: Header-Value`

**示例**：
```
Content-Type: application/json
Authorization: Bearer your-token-here
X-Custom-Header: custom-value
```

**常用请求头**：
- `Content-Type`: 指定请求体格式（`application/json`, `application/x-www-form-urlencoded`）
- `Authorization`: 身份认证令牌
- `User-Agent`: 客户端标识
- `Accept`: 接受的响应格式

**注意事项**：
- 以 `#` 开头的行会被忽略（注释）
- 空行会被跳过
- 请求头名称不区分大小写

## bodyTemplate

**请求体模板**

使用 `${paramName}` 格式引用 Action 参数值。

**JSON 格式示例**：
```json
{
  "event": "data_quality_issue",
  "issue": {
    "type": "${issueType}",
    "table": "${tableName}",
    "affectedRows": ${affectedRows}
  },
  "timestamp": "${timestamp}"
}
```

**Form 格式示例**：
```
issueType=${issueType}&tableName=${tableName}&affectedRows=${affectedRows}
```

**占位符替换规则**：
- 参数名大小写敏感
- 如果参数不存在，占位符保持原样
- 数值类型不需要加引号（JSON 格式）

## httpMethod

**HTTP 请求方法**

支持的方法：
- **POST**（默认）: 创建资源或提交数据
- **PUT**: 更新资源
- **PATCH**: 部分更新资源
- **DELETE**: 删除资源
- **GET**: 查询资源（不推荐，GET 请求通常不携带请求体）

**选择建议**：
- 触发外部操作 → POST
- 更新外部状态 → PUT/PATCH
- 通知外部系统 → POST

## retryOnFailure

**失败重试机制**

- **false（默认）**: 不重试
  - 请求失败后立即返回错误
  - 适用于幂等性要求高的场景
  
- **true**: 自动重试
  - 失败后延迟重试一次
  - 适用于网络抖动或临时故障场景

**重试策略**：
- 重试次数：1 次
- 重试延迟：2 秒
- 仅在网络错误或 5xx 状态码时重试
- 4xx 状态码（客户端错误）不重试

**注意事项**：
- 确保 Webhook 接口支持幂等性
- 避免重复触发外部副作用
- 超时时间会影响总执行时长
