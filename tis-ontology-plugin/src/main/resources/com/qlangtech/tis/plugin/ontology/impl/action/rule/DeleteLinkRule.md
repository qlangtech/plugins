## 删除链接规则

删除两个对象之间的一条链接。

### 链接类型以 Action 为准

`LinkActionType#linkTypeReference`（Action 详情页的「链接类型」）是唯一权威来源。
规则上的 `linkTypeName` 仅为兼容旧配置保留：Action 级为空时才回退到它；
两者都填但不一致会直接报错，避免按错误的链接类型执行。

### 删除如何落表

按链接类型声明的 `RelationshipType` 翻译成对底层表的删除：

| RelationshipType | 删除方式 |
|---|---|
| `ObjectTypeForeignKeys` | `UPDATE 源表 SET left.targetField = NULL WHERE 源主键 = sourceRid` |
| `JoinTableDataset` | `DELETE FROM 连接表 WHERE join.targetField = <源值> AND join.rightTargetField = <目标值>` |
| `BackingObjectType` | 同 `JoinTableDataset`，两端值固定为左右对象的主键 |

补充约定：

- 目标行按**目标对象主键**定位，连接值取自 `right.targetField`；
- 外键当前值不等于目标值、或连接行不存在时，报 `link not found`，**不会静默成功**；
- 外键列同时是源对象主键（1:1 依赖表）时无法置空，会提示改为删除对象。

### deleteBidirectional

**不支持取值为 true。**

`deleteBidirectional=true` 会在执行任何 DML 之前抛出 `UnsupportedOperationException`。
TIS 没有链接实例存储，不存在 `linkTypeName + "_reverse"` 这样的反向链接类型。

### 参数名

- `sourceObjectRidParam`（默认 `sourceRid`）：参数池里承载源对象 RID 的字段；
- `targetObjectRidParam`（默认 `targetRid`）：承载目标对象 RID 的字段。

Workshop 模态框会用选中行的 `__rid` 预填源字段，并通过目标对象选择器填写目标字段。
