## functionId

**函数标识符**

指定要调用的 Ontology Function 的唯一标识。

**函数类型**：
- **Ontology Functions**: 在 Ontology 中定义的业务函数
- **AIP Logic Functions**: AIP Logic 平台定义的逻辑函数
- **自定义 Functions**: 项目中注册的自定义函数

**示例**：
```
calculateDataQualityScore
validateDataIntegrity
enrichDataWithExternalSource
```

## parameterMappings

**参数映射配置**

将 Action 参数映射到函数输入参数，每行一个映射，格式：`functionParam=actionParam`

**示例**：

假设函数签名为：
```typescript
function calculateScore(tableName: string, rowCount: number): number
```

Action 参数为：
```json
{
  "table": "user_orders",
  "rows": 1000
}
```

参数映射配置：
```
tableName=table
rowCount=rows
```

执行时会将：
- Action 的 `table` 参数值 → 函数的 `tableName` 参数
- Action 的 `rows` 参数值 → 函数的 `rowCount` 参数

**映射规则**：
- 左侧：函数参数名（Function 定义的参数）
- 右侧：Action 参数名（用户传入的参数）
- 以 `#` 开头的行为注释
- 空行会被跳过

**高级用法**：

直接传递常量值（使用特殊前缀 `const:`）：
```
threshold=const:0.8
mode=const:strict
```

## async

**异步执行模式**

控制函数执行方式：

- **false（默认）**: 同步执行
  - Action 等待函数执行完成后再返回
  - 函数结果会包含在 Action 响应中
  - 适用于：
    - 快速计算（< 5 秒）
    - 需要立即使用函数返回值的场景
    - 数据验证、格式转换等
  
- **true**: 异步执行
  - Action 立即返回，函数在后台执行
  - 不会阻塞 Action 的完成
  - 适用于：
    - 长时间运行的任务（> 10 秒）
    - 外部 API 调用
    - 批量数据处理
    - 不需要立即获取结果的场景

**异步执行注意事项**：
- 函数执行状态需要通过其他方式查询
- 函数执行失败不会影响 Action 成功状态
- 建议配合通知规则使用，及时获取执行结果

## timeoutSeconds

**超时时间（秒）**

函数执行的最大允许时间，超时后会终止执行。

**推荐值**：
- **快速计算**（数据验证、格式转换）：5-10 秒
- **数据库查询**（复杂聚合、关联）：30-60 秒
- **外部 API 调用**（HTTP 请求）：30-60 秒
- **批量处理**（大规模数据操作）：120-300 秒

**注意事项**：
- 同步执行时，超时会导致 Action 失败
- 异步执行时，超时会终止函数但不影响 Action
- 设置过长可能影响系统资源
- 建议根据实际测试调整

**错误处理**：

超时错误示例：
```
Function execution timeout after 60 seconds: functionId=calculateScore
```
