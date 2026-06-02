# AstValidator WITH 语句支持修复

## 问题描述

测试 `TestAstValidator.testValidSelectForWithStatement()` 失败，因为 `AstValidator` 没有正确处理 SQL 中的 WITH 语句（CTE - Common Table Expression）。

## 根本原因

1. **CTE 表名被当作普通表校验**：WITH 语句中定义的临时表（如 `category_sales`、`total_sales`）被当作需要在白名单中的普通表
2. **分号解析错误**：Presto SQL Parser 不接受 SQL 末尾的分号
3. **CTE 之间的 JOIN 被校验**：CTE 定义的临时表之间的 JOIN 也被当作需要 linker 的 JOIN
4. **CROSS JOIN 误报**：没有 ON 条件的 CROSS JOIN（包括逗号分隔的多表查询）被当作需要 linker 的 JOIN

## 解决方案

### 1. 支持 WITH 语句中的 CTE（核心修复）

**修改文件**：`AstValidator.java`

在 `AstCollector` 类中添加：
- 新增 `cteNames` 集合，用于存储 CTE 定义的表名
- 重写 `visitWith()` 方法，收集所有 CTE 名称
- 修改 `visitTable()` 方法，排除 CTE 表名的收集
- 修改 `visitJoin()` 方法，排除涉及 CTE 的 JOIN

```java
final Set<String> cteNames = new HashSet<>();

@Override
protected Void visitWith(With node, Void context) {
    for (WithQuery query : node.getQueries()) {
        cteNames.add(query.getName().toString());
    }
    return super.visitWith(node, context);
}

@Override
protected Void visitTable(Table node, Void context) {
    String tableName = node.getName().toString();
    if (!cteNames.contains(tableName)) {
        tables.add(tableName);
    }
    return super.visitTable(node, context);
}
```

### 2. 自动去除末尾分号

在 `validate()` 方法开头添加：

```java
// 去除末尾的分号（Presto Parser 不接受分号）
sql = sql.trim();
if (sql.endsWith(";")) {
    sql = sql.substring(0, sql.length() - 1).trim();
}
```

### 3. 跳过 CROSS JOIN 的 linker 校验

修改 `visitJoin()` 方法，只对有 ON 条件的 JOIN 进行 linker 校验：

```java
if (left != null && right != null && !cteNames.contains(left) && !cteNames.contains(right)) {
    // 如果有 JOIN 条件（不是 CROSS JOIN），才需要校验 linker
    if (node.getCriteria().isPresent()) {
        joins.add(new JoinPair(left, right));
    }
}
```

### 4. 修复测试断言

**修改文件**：`TestAstValidator.java`

- `testValidSelectForWithStatement`：添加必要的 linker（`toy_sales <-> toy_products`）
- `testInvalidTableNotInWhitelist`：修改断言检查 `issues` 而不是 `reason`

```java
// 修改前
assertTrue("Error should mention invalid table", result.reason().contains("Invalid tables"));

// 修改后
assertTrue("Error should mention invalid table",
        result.issues().stream().anyMatch(issue -> issue.contains("Invalid tables")));
```

## 测试结果

所有 12 个测试全部通过：

```
[INFO] Tests run: 12, Failures: 0, Errors: 0, Skipped: 0
```

## 支持的 SQL 特性

修复后，`AstValidator` 现在支持：

1. ✅ **WITH 语句（CTE）**：可以定义和使用多个 CTE
2. ✅ **嵌套 CTE**：CTE 可以引用其他 CTE
3. ✅ **CTE 与真实表的 JOIN**：CTE 中可以 JOIN 真实表
4. ✅ **CROSS JOIN**：不需要 linker 的笛卡尔积 JOIN
5. ✅ **逗号分隔的多表查询**：隐式 CROSS JOIN
6. ✅ **带分号的 SQL**：自动去除末尾分号

## 示例

### 成功案例：复杂 WITH 语句

```sql
WITH category_sales AS (
  SELECT 
    p.Product_Category,
    SUM(CAST(p.Product_Price AS DECIMAL) * s.Units) AS category_total_sales
  FROM toy_sales s
  JOIN toy_products p ON s.Product_ID = p.Product_ID
  GROUP BY p.Product_Category
  HAVING SUM(CAST(p.Product_Price AS DECIMAL) * s.Units) > 50
),
total_sales AS (
  SELECT SUM(category_total_sales) AS grand_total
  FROM category_sales
)
SELECT 
  cs.Product_Category,
  cs.category_total_sales,
  ROUND(cs.category_total_sales / ts.grand_total * 100, 2) AS percentage_of_total
FROM category_sales cs
CROSS JOIN total_sales ts;
```

**校验结果**：✅ 通过
- CTE `category_sales` 和 `total_sales` 不在白名单校验范围内
- 真实表 `toy_sales` 和 `toy_products` 的 JOIN 通过 linker 校验
- CROSS JOIN 不需要 linker 校验

## 相关文件

- `AstValidator.java` - 核心校验逻辑
- `TestAstValidator.java` - 单元测试
- `ValidationResult.java` - 校验结果数据结构

## 日期

2026-06-02