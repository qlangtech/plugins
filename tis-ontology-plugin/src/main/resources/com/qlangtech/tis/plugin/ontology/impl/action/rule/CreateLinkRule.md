## bidirectional

**双向链接模式**

控制链接的方向性：

- **false（默认）**: 单向链接
  - 仅创建从源对象到目标对象的链接
  - 例如：A → B（A 指向 B）
  - 适用于明确的单向关系，如 "拥有"、"包含"
  
- **true**: 双向链接
  - 同时创建两个方向的链接
  - 例如：A ↔ B（A 指向 B，B 也指向 A）
  - 适用于对称关系，如 "关联"、"相关"

**实现细节**：

当 `bidirectional=true` 时，系统会自动创建两条链接记录：
1. 正向链接：使用指定的 `linkTypeName`
2. 反向链接：使用 `linkTypeName + "_reverse"` 作为链接类型

**示例**：

假设要创建 "DataAsset" 和 "DataSource" 之间的关联关系：

单向链接（`bidirectional=false`）：
```
参数:
{
  "linkTypeName": "belongsTo",
  "sourceRid": "asset_001",
  "targetRid": "source_001"
}

结果:
asset_001 --belongsTo--> source_001
```

双向链接（`bidirectional=true`）：
```
参数:
{
  "linkTypeName": "relatesTo",
  "sourceRid": "asset_001",
  "targetRid": "source_001",
  "bidirectional": true
}

结果:
asset_001 --relatesTo--> source_001
source_001 --relatesTo_reverse--> asset_001
```

**注意事项**：
- 双向链接会创建两倍的链接记录
- 删除时需要同时删除两个方向的链接
- 确保 Ontology 中已定义对应的链接类型
