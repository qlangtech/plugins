## deleteBidirectional

**双向链接删除模式**

控制删除链接时是否同时删除反向链接：

- **false（默认）**: 仅删除单向链接
  - 只删除从源对象到目标对象的链接
  - 例如：删除 A → B，但保留 B → A（如果存在）
  - 适用于单向链接或需要保留反向链接的场景
  
- **true**: 同时删除双向链接
  - 删除从源对象到目标对象的链接
  - 同时删除从目标对象到源对象的反向链接
  - 适用于通过 CreateLinkRule 创建的双向链接

**实现细节**：

当 `deleteBidirectional=true` 时，系统会删除两条链接记录：
1. 正向链接：使用指定的 `linkTypeName`
2. 反向链接：使用 `linkTypeName + "_reverse"` 作为链接类型

**示例**：

假设存在以下链接：
```
asset_001 --belongsTo--> source_001
```

单向删除（`deleteBidirectional=false`）：
```
参数:
{
  "linkTypeName": "belongsTo",
  "sourceRid": "asset_001",
  "targetRid": "source_001"
}

结果: 仅删除 asset_001 → source_001
```

双向删除（`deleteBidirectional=true`）：
```
参数:
{
  "linkTypeName": "relatesTo",
  "sourceRid": "asset_001",
  "targetRid": "source_001",
  "deleteBidirectional": true
}

结果:
- 删除 asset_001 --relatesTo--> source_001
- 删除 source_001 --relatesTo_reverse--> asset_001
```

**注意事项**：
- 必须确保链接类型名称与创建时一致
- 如果链接不存在，删除操作会静默失败（不会报错）
- 双向删除需要确保反向链接确实存在
