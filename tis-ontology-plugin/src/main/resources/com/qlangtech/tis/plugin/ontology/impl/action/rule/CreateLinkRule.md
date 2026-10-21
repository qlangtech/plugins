## 创建链接规则

在两个对象之间创建一条链接。

### 链接类型以 Action 为准

`LinkActionType#linkTypeReference`（Action 详情页的「链接类型」）是唯一权威来源。
规则上的 `linkTypeName` 仅为兼容旧配置保留：Action 级为空时才回退到它；
两者都填但不一致会直接报错，避免按错误的链接类型执行。

### 链接实例落在哪里

TIS 没有链接实例表，`OntologyLinker` 只是声明式元数据。创建链接时会按链接类型声明的
`RelationshipType` 翻译成对底层表的写入：

| RelationshipType | 创建方式 |
|---|---|
| `ObjectTypeForeignKeys` | `UPDATE 源表 SET left.targetField = <目标行 right.targetField 的值> WHERE 源主键 = sourceRid` |
| `JoinTableDataset` | 向连接表 `INSERT (join.targetField, join.rightTargetField)`，值分别取自源行 / 目标行 |
| `BackingObjectType` | 同 `JoinTableDataset`，但两端连接值固定为左右对象的主键 |

补充约定：

- 目标行按**目标对象主键**定位，写入的连接值取自 `right.targetField` —— 二者可能不是同一列；
- 重复创建（外键已指向同一目标 / 连接行已存在）幂等成功；
- 外键已指向**另一个**目标时报「请先删除现有链接」，不会静默覆盖；
- `cardinality=ONE_ONE` 时，创建前会检查是否已有其它源对象指向同一目标。

### bidirectional

**不支持取值为 true。**

`bidirectional=true` 会在执行任何 DML 之前抛出 `UnsupportedOperationException`。
原因同上：没有链接实例存储，也就不存在「反向链接」，更不存在 `linkTypeName + "_reverse"`
这样的链接类型。

### 参数名

- `sourceObjectRidParam`（默认 `sourceRid`）：参数池里承载源对象 RID 的字段；
- `targetObjectRidParam`（默认 `targetRid`）：承载目标对象 RID 的字段。

Workshop 模态框会用选中行的 `__rid` 预填源字段，并通过目标对象选择器填写目标字段。
参数池缺少这两个参数时，模态框会在打开时直接提示配置错误。
