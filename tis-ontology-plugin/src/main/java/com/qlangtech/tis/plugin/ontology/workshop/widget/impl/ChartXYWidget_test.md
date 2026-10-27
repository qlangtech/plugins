# ChartXY 组件手工测试手册

本文件给出一套**可重复执行**的手工测试方法：在 `order2` 库里造三张固定形状的表，把它们建成
本体对象类型与 Workshop 对象集变量，再用 ChartXY 的柱状 / 折线 / 散点三种画法各配一遍，
并逐一核对返回的分桶与聚合值。

所有预期值都是**对着 `192.168.28.200/order2` 实测**得来的（2026-10-27），不是估算。
若实测不一致，先怀疑配置错了（属性名拼错、分桶粒度选错），再怀疑代码。

---

## 1. 环境前置

| 项 | 值 |
|---|---|
| MySQL | `192.168.28.200:3306`，5.7.30-log，账号 `root/123456` |
| 库 | `order2`（**库级字符集是 `latin1`，但下面三张表显式声明 `utf8mb4`**）|
| TIS 数据源 | 名 `order2`，实现 `MySQLV5DataSourceFactory`，配置文件在 `{pluginCfgRoot}/db/order2/` |
| Ontology 域 | `test`（`{pluginCfgRoot}/com.qlangtech.tis.plugin.ontology.OntologyDomain.xml` 里只有它一个）|
| `{pluginCfgRoot}` | `/opt/data/tis/cfg_repo/tis_plugin_config/` |

连接命令：

```bash
mysql -h192.168.28.200 -uroot -p123456 order2
```

> 该实例以 `--skip-grant-tables` 启动，`SHOW GRANTS` 会报错，这是环境特性、不是权限问题。

### 1.1 造数脚本

脚本是**纯 SQL、幂等**（先 `DROP TABLE IF EXISTS` 再 `CREATE`），可以反复执行。
独立文件与本文同目录：**`ChartXYWidget_test_seed.sql`**（下面也内嵌了一份全文，方便直接复制）。

```bash
cd /Users/mozhenghua/j2ee_solution/project/plugins/tis-ontology-plugin/src/main/java/com/qlangtech/tis/plugin/ontology/workshop/widget/impl
mysql -h192.168.28.200 -uroot -p123456 order2 < ChartXYWidget_test_seed.sql
```

```sql
SET NAMES utf8mb4;
DROP TABLE IF EXISTS chartxy_order_fact;
DROP TABLE IF EXISTS chartxy_server_metric;
DROP TABLE IF EXISTS chartxy_daily_report;

CREATE TABLE chartxy_order_fact (
  order_id     BIGINT      NOT NULL AUTO_INCREMENT COMMENT '订单行主键',
  order_time   DATETIME    NOT NULL COMMENT '下单时间（含时分秒，可做小时粒度分桶）',
  order_date   DATE        NOT NULL COMMENT '下单日期（无时分秒，只能做到天粒度）',
  region       VARCHAR(32) NOT NULL COMMENT '大区（低基数分类维度）',
  channel      VARCHAR(32) NOT NULL COMMENT '销售渠道（低基数分类维度，用作 segmentBy）',
  product_line VARCHAR(32) NOT NULL COMMENT '产品线（中等基数分类维度）',
  quantity     INT         NOT NULL COMMENT '购买件数（整数度量）',
  unit_price   DECIMAL(10,2) NOT NULL COMMENT '单价（定点小数度量）',
  amount       DOUBLE      NOT NULL COMMENT '订单金额（浮点度量）',
  cost         DOUBLE      DEFAULT NULL COMMENT '成本（含 NULL：验证缺口/空值显示）',
  customer_id  BIGINT      NOT NULL COMMENT '客户 ID（用于 COUNT DISTINCT）',
  is_paid      INT         NOT NULL DEFAULT 1 COMMENT '是否已支付（1=已付，0=未付）',
  PRIMARY KEY (order_id),
  KEY idx_order_time (order_time),
  KEY idx_order_date (order_date),
  KEY idx_region (region)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ChartXY 测试：订单事实表（bar / scatter）';

INSERT INTO chartxy_order_fact
  (order_id, order_time, order_date, region, channel, product_line,
   quantity, unit_price, amount, cost, customer_id, is_paid)
SELECT
  oid,
  STR_TO_DATE(CONCAT('2026-', m, '-', ELT(k+1,5,15,25), ' ',
      LPAD(8 + MOD(m*3 + ri*5 + ci*7 + k*11, 12),2,'0'), ':',
      LPAD(MOD(m*7 + ri*13 + ci*17 + k*23, 60),2,'0'), ':',
      LPAD(MOD(m*11 + k*29, 60),2,'0')), '%Y-%c-%e %H:%i:%s'),
  DATE(CONCAT('2026-', m, '-', ELT(k+1,5,15,25))),
  ELT(ri+1,'华东','华北','华南','西南'),
  ELT(ci+1,'线上','门店','分销'),
  ELT(MOD(ri+ci+k,3)+1,'标准版','专业版','旗舰版'),
  qty,
  price,
  ROUND(price * qty, 2),
  CASE WHEN MOD(oid,4) = 0 THEN NULL
       ELSE ROUND(ROUND(price * qty, 2) * (0.55 + 0.05 * MOD(oid,5)), 2) END,
  5000 + MOD(oid*17, 97) + ELT(MOD(ri+ci+k,3)+1, 0, 500, 1000),
  CASE WHEN MOD(oid,5) = 0 THEN 0 ELSE 1 END
FROM (
  SELECT
    ((m-1)*36 + ri*9 + ci*3 + k + 1) AS oid,
    m, ri, ci, k,
    MOD(m*7 + ri*5 + ci*3 + k*2, 20) + 1 AS qty,
    ROUND((99 + ri*40 + ci*25) * ELT(ri+1,1.0,0.9,1.15,0.8), 2) AS price
  FROM (SELECT 1 AS m UNION ALL SELECT 2 AS m UNION ALL SELECT 3 AS m UNION ALL SELECT 4 AS m UNION ALL SELECT 5 AS m UNION ALL SELECT 6 AS m UNION ALL SELECT 7 AS m UNION ALL SELECT 8 AS m UNION ALL SELECT 9 AS m UNION ALL SELECT 10 AS m UNION ALL SELECT 11 AS m UNION ALL SELECT 12 AS m) m
  CROSS JOIN (SELECT 0 AS ri UNION ALL SELECT 1 AS ri UNION ALL SELECT 2 AS ri UNION ALL SELECT 3 AS ri) ri
  CROSS JOIN (SELECT 0 AS ci UNION ALL SELECT 1 AS ci UNION ALL SELECT 2 AS ci) ci
  CROSS JOIN (SELECT 0 AS k UNION ALL SELECT 1 AS k UNION ALL SELECT 2 AS k) k
) t ORDER BY oid;


CREATE TABLE chartxy_server_metric (
  metric_id   BIGINT      NOT NULL AUTO_INCREMENT COMMENT '监控点主键',
  event_time  DATETIME    NOT NULL COMMENT '采集时间（整点对齐，可做小时粒度分桶）',
  host        VARCHAR(32) NOT NULL COMMENT '主机名（低基数分类维度）',
  region      VARCHAR(32) NOT NULL COMMENT '机房（低基数分类维度，用作 segmentBy）',
  cpu_usage   DECIMAL(6,2) NOT NULL COMMENT 'CPU 使用率（%）',
  mem_usage   DECIMAL(6,2) NOT NULL COMMENT '内存使用率（%）',
  qps         BIGINT      NOT NULL COMMENT '每秒查询数',
  err_count   INT         NOT NULL COMMENT '错误数',
  PRIMARY KEY (metric_id),
  KEY idx_event_time (event_time),
  KEY idx_host (host)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ChartXY 测试：主机监控时序表（line / scatter）';

INSERT INTO chartxy_server_metric (metric_id, event_time, host, region, cpu_usage, mem_usage, qps, err_count)
SELECT
  mid,
  TIMESTAMPADD(HOUR, h, TIMESTAMPADD(DAY, d, '2026-09-01 00:00:00')),
  ELT(hi+1,'host-a','host-b','host-c'),
  ELT(hi+1,'北京机房','北京机房','上海机房'),
  LEAST(99, ROUND(30 + 25*ABS(12-h)/12.0 + hi*7 + MOD(d,3)*2 + MOD(mid,5), 2)),
  LEAST(99, ROUND(45 + hi*9 + h*0.7 + MOD(mid,4), 2)),
  FLOOR(800 + (24-h)*35 + hi*120 + MOD(mid,17)*6),
  MOD(mid*13, 9)
FROM (
  SELECT d, h, hi, d*72 + h*3 + hi + 1 AS mid
  FROM (SELECT 0 AS d UNION ALL SELECT 1 AS d UNION ALL SELECT 2 AS d UNION ALL SELECT 3 AS d UNION ALL SELECT 4 AS d UNION ALL SELECT 5 AS d UNION ALL SELECT 6 AS d) d
  CROSS JOIN (SELECT 0 AS h UNION ALL SELECT 1 AS h UNION ALL SELECT 2 AS h UNION ALL SELECT 3 AS h UNION ALL SELECT 4 AS h UNION ALL SELECT 5 AS h UNION ALL SELECT 6 AS h UNION ALL SELECT 7 AS h UNION ALL SELECT 8 AS h UNION ALL SELECT 9 AS h UNION ALL SELECT 10 AS h UNION ALL SELECT 11 AS h UNION ALL SELECT 12 AS h UNION ALL SELECT 13 AS h UNION ALL SELECT 14 AS h UNION ALL SELECT 15 AS h UNION ALL SELECT 16 AS h UNION ALL SELECT 17 AS h UNION ALL SELECT 18 AS h UNION ALL SELECT 19 AS h UNION ALL SELECT 20 AS h UNION ALL SELECT 21 AS h UNION ALL SELECT 22 AS h UNION ALL SELECT 23 AS h) h
  CROSS JOIN (SELECT 0 AS hi UNION ALL SELECT 1 AS hi UNION ALL SELECT 2 AS hi) hi
) t ORDER BY mid;


CREATE TABLE chartxy_daily_report (
  report_id    BIGINT      NOT NULL AUTO_INCREMENT COMMENT '日报主键',
  report_date  DATE        NOT NULL COMMENT '报表日期（仅日期，无时间部分）',
  department   VARCHAR(32) NOT NULL COMMENT '部门（低基数分类维度）',
  task_total   INT         NOT NULL COMMENT '任务总数',
  task_success INT         NOT NULL COMMENT '成功数',
  task_failed  INT         NOT NULL COMMENT '失败数',
  duration_sec DOUBLE      DEFAULT NULL COMMENT '平均耗时（秒），可为空',
  PRIMARY KEY (report_id),
  KEY idx_report_date (report_date)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='ChartXY 测试：部门日报表（DATE 分桶边界）';

INSERT INTO chartxy_daily_report (report_id, report_date, department, task_total, task_success, task_failed, duration_sec)
SELECT
  rid,
  DATE_ADD('2026-07-01', INTERVAL d DAY),
  ELT(di+1,'数据平台','风控','结算'),
  total,
  total - failed,
  failed,
  CASE WHEN MOD(rid,11) = 0 THEN NULL ELSE ROUND(12.5 + MOD(rid,23)*0.9, 2) END
FROM (
  SELECT d, di, d*3 + di + 1 AS rid,
         120 + MOD(d*3 + di*17, 180) AS total,
         MOD(d*7 + di*5, 12) AS failed
  FROM (SELECT 0 AS d UNION ALL SELECT 1 AS d UNION ALL SELECT 2 AS d UNION ALL SELECT 3 AS d UNION ALL SELECT 4 AS d UNION ALL SELECT 5 AS d UNION ALL SELECT 6 AS d UNION ALL SELECT 7 AS d UNION ALL SELECT 8 AS d UNION ALL SELECT 9 AS d UNION ALL SELECT 10 AS d UNION ALL SELECT 11 AS d UNION ALL SELECT 12 AS d UNION ALL SELECT 13 AS d UNION ALL SELECT 14 AS d UNION ALL SELECT 15 AS d UNION ALL SELECT 16 AS d UNION ALL SELECT 17 AS d UNION ALL SELECT 18 AS d UNION ALL SELECT 19 AS d UNION ALL SELECT 20 AS d UNION ALL SELECT 21 AS d UNION ALL SELECT 22 AS d UNION ALL SELECT 23 AS d UNION ALL SELECT 24 AS d UNION ALL SELECT 25 AS d UNION ALL SELECT 26 AS d UNION ALL SELECT 27 AS d UNION ALL SELECT 28 AS d UNION ALL SELECT 29 AS d UNION ALL SELECT 30 AS d UNION ALL SELECT 31 AS d UNION ALL SELECT 32 AS d UNION ALL SELECT 33 AS d UNION ALL SELECT 34 AS d UNION ALL SELECT 35 AS d UNION ALL SELECT 36 AS d UNION ALL SELECT 37 AS d UNION ALL SELECT 38 AS d UNION ALL SELECT 39 AS d UNION ALL SELECT 40 AS d UNION ALL SELECT 41 AS d UNION ALL SELECT 42 AS d UNION ALL SELECT 43 AS d UNION ALL SELECT 44 AS d UNION ALL SELECT 45 AS d UNION ALL SELECT 46 AS d UNION ALL SELECT 47 AS d UNION ALL SELECT 48 AS d UNION ALL SELECT 49 AS d UNION ALL SELECT 50 AS d UNION ALL SELECT 51 AS d UNION ALL SELECT 52 AS d UNION ALL SELECT 53 AS d UNION ALL SELECT 54 AS d UNION ALL SELECT 55 AS d UNION ALL SELECT 56 AS d UNION ALL SELECT 57 AS d UNION ALL SELECT 58 AS d UNION ALL SELECT 59 AS d UNION ALL SELECT 60 AS d UNION ALL SELECT 61 AS d UNION ALL SELECT 62 AS d UNION ALL SELECT 63 AS d UNION ALL SELECT 64 AS d UNION ALL SELECT 65 AS d UNION ALL SELECT 66 AS d UNION ALL SELECT 67 AS d UNION ALL SELECT 68 AS d UNION ALL SELECT 69 AS d UNION ALL SELECT 70 AS d UNION ALL SELECT 71 AS d UNION ALL SELECT 72 AS d UNION ALL SELECT 73 AS d UNION ALL SELECT 74 AS d UNION ALL SELECT 75 AS d UNION ALL SELECT 76 AS d UNION ALL SELECT 77 AS d UNION ALL SELECT 78 AS d UNION ALL SELECT 79 AS d UNION ALL SELECT 80 AS d UNION ALL SELECT 81 AS d UNION ALL SELECT 82 AS d UNION ALL SELECT 83 AS d UNION ALL SELECT 84 AS d UNION ALL SELECT 85 AS d UNION ALL SELECT 86 AS d UNION ALL SELECT 87 AS d UNION ALL SELECT 88 AS d UNION ALL SELECT 89 AS d UNION ALL SELECT 90 AS d) d
  CROSS JOIN (SELECT 0 AS di UNION ALL SELECT 1 AS di UNION ALL SELECT 2 AS di) di
) t ORDER BY rid;
```

执行后立刻自检（三个数字必须对上）：

```sql
select 'order_fact' t, count(*) from chartxy_order_fact
union all select 'server_metric', count(*) from chartxy_server_metric
union all select 'daily_report', count(*) from chartxy_daily_report;
-- 期望 432 / 504 / 273
```

---

## 2. 三张表的设计意图

三张表不是随便造的，各自钉住一类测试点：

| 表 | 行数 | 钉住什么 |
|---|---|---|
| `chartxy_order_fact` | 432 | 分类轴（低基数）、二次分段（`region` × `channel`）、含时分秒的 `DATETIME` 与纯日期 `DATE` 并存、`cost` 有 108 行 NULL、`customer_id` 跨区重叠（总数 257 ≠ 各区和） |
| `chartxy_server_metric` | 504 | 连续 7 天 × 24 小时整点数据（小时粒度分桶、单位数 6 的粗粒度）、共线度量（`qps` 随小时单调变化，便于肉眼确认分桶没串） |
| `chartxy_daily_report` | 273 | 91 天连续日期（天/周/月/季/年五种粒度全覆盖）、`duration_sec` 有 24 行 NULL、数据从 2026-07-01 开始（周粒度第一个桶是残缺桶） |

几个**故意的陷阱**，测的时候要留意：

1. **`chartxy_order_fact` 是完形平衡的**：12 个月 × 4 区 × 3 渠道 × 3 天 = 432，每个
   `region × channel` 单元格都是 36 行。所以**按 Count 聚合的画，各柱子一样高**——这不是 bug。
   要看出差异必须用 `SUM(amount)` / `MAX(amount)` / `AVG(cost)` 这类度量。
2. **`cost` 有 108 行 NULL**（`MOD(order_id,4)=0`），`COUNT(cost)=324`。
   `AVG` / `SUM` 会跳过 NULL（`SUM(cost)` 只累加 324 行），`COUNT` 不受影响——
   这是验证「聚合函数语义对不对」最直接的一处。
3. **`duration_sec` 有 24 行 NULL**，`COUNT(duration_sec)=249`。
4. **`chartxy_daily_report` 从 2026-07-01 开始**：周粒度（`unitValue=2`）第一个桶
   `2026-06-22` 只有 15 行，是残缺桶；最后一个桶 `2026-09-28` 只有 6 行。这是**刻意**的，
   用来确认分桶按自然周起点对齐、而不是按第一行数据对齐。
5. **`chartxy_order_fact` 里 `order_date` 是 `DATE`、`order_time` 是 `DATETIME`**。
   同一个图层属性选错字段，能配出来但粒度上不去（见 §7 负例）。

### 2.1 列 → OntologyType 映射

建对象类型时属性类型是**自动推断**的（`OntologyPropertyCreatorFactory.appendExternalJsonProp`
调 `OntologyType.convert(col.getType())`），下面是应有的结果，用来核对自动推断是否正常：

| 表.列 | MySQL 类型 | OntologyType | 可做 X 轴分桶 | 可做 SUM/AVG/MIN/MAX |
|---|---|---|---|---|
| `chartxy_order_fact.order_id` | `bigint` | `LONG` | 分类 | 是 |
| `chartxy_order_fact.order_time` | `datetime` | `TIMESTAMP` | **时间粒度（全 8 级）** | — |
| `chartxy_order_fact.order_date` | `date` | `DATE` | **时间粒度（只到 DAY）** | — |
| `chartxy_order_fact.region` | `varchar` | `STRING` | 分类 | 否 |
| `chartxy_order_fact.channel` | `varchar` | `STRING` | 分类 | 否 |
| `chartxy_order_fact.product_line` | `varchar` | `STRING` | 分类 | 否 |
| `chartxy_order_fact.quantity` | `int` | `INTEGER` | 分类 | 是 |
| `chartxy_order_fact.unit_price` | `decimal` | `DECIMAL` | 分类 | 是 |
| `chartxy_order_fact.amount` | `double` | `DOUBLE` | 分类 | 是 |
| `chartxy_order_fact.cost` | `double`（可空） | `DOUBLE` | 分类 | 是 |
| `chartxy_order_fact.customer_id` | `bigint` | `LONG` | 分类 | 是 |
| `chartxy_order_fact.is_paid` | `int` | `INTEGER` | 分类 | 是 |
| `chartxy_server_metric.metric_id` | `bigint` | `LONG` | 分类 | 是 |
| `chartxy_server_metric.event_time` | `datetime` | `TIMESTAMP` | **时间粒度（全 8 级）** | — |
| `chartxy_server_metric.host` / `region` | `varchar` | `STRING` | 分类 | 否 |
| `chartxy_server_metric.cpu_usage` / `mem_usage` | `decimal(6,2)` | `DECIMAL` | 分类 | 是 |
| `chartxy_server_metric.qps` | `bigint` | `LONG` | 分类 | 是 |
| `chartxy_server_metric.err_count` | `int` | `INTEGER` | 分类 | 是 |
| `chartxy_daily_report.report_date` | `date` | `DATE` | **只到 DAY** | — |
| `chartxy_daily_report.department` | `varchar` | `STRING` | 分类 | 否 |
| `chartxy_daily_report.task_total` / `task_success` / `task_failed` | `int` | `INTEGER` | 分类 | 是 |
| `chartxy_daily_report.duration_sec` | `double`（可空） | `DOUBLE` | 分类 | 是 |

> `tinyint` 会推成 `SHORT`、`varchar` 一律 `STRING`。`DATETIME` 与 `DATE` 的差别**只体现在
> 能不能用 HOUR/MINUTE/SECOND 粒度**，两者都是 `TIMESTAMP` / `DATE` 两个不同的 `OntologyType`。

---

## 3. 建三张表的本体对象类型

入口：**Ontology → 对象类型 → 新建**，是一个 3 步向导。

### 3.1 第 1 步「属性设置」（`ObjectTypeProfile`）

| 字段 | 三张表分别填 |
|---|---|
| 对象名称 `name` | `chartxy_order_fact` / `chartxy_server_metric` / `chartxy_daily_report` |
| 资源绑定 `binding` | 选 `DataSource Bound`（不是 `Unbound`）→ 数据源 `order2` → 目标表选对应表 |
| 别名 `alias` | `chartxy_order_fact_alias` / …（**同 `name` 一样受 `Validator.identity` 约束**）|
| 描述 `description` | 随便写中文，如「ChartXY 测试：订单事实表」 |

> ⚠️ `name` 与 `alias` 都带 `Validator.identity`，只允许 `[A-Za-z0-9_-]`。
> 中文名会被拒（提示「必须由小写字母，大写字母，数字、下划线、减号组成」）。

### 3.2 第 2 步「基本信息设置」（`ObjectTypeProperties`）

点进这一步时属性表格会**自动按物理表列填充**（`binding.resolveTabCols()`）。逐行核对：

- 属性名与 §2.1 一致；
- 类型列与 §2.1 的 `OntologyType` 列一致；
- 主键列会被预标记（`order_id` / `metric_id` / `report_id`）。

一般不需要改动。若某列类型推断错了（比如 `decimal` 被推成 `DOUBLE`），先查数据源的
`ColumnMetaData`，不要在表格里手改。

### 3.3 第 3 步「属性相关设置」（`ObjectTypePropertiesRelevant`）

| 字段 | 各表填法 |
|---|---|
| 主键设置 `pkSetter` | 选「开」→ `pkField` 选 `order_id` / `metric_id` / `report_id` |
| 标题键 `titleKey` | 各表建议：`region` / `host` / `department`（**受 `Validator.db_col_name` 约束，必须是合法列名**）|

保存后落盘位置：`{pluginCfgRoot}/ontology/test/object-type/`。

---

## 4. 建三个对象集变量

入口：**Workshop → 变量 → 新建**，类型选「对象集合」（`OBJECT_SET`）。

| 字段 | 三张表分别填 |
|---|---|
| 变量名称 `name` | `order_fact_set` / `server_metric_set` / `daily_report_set` |
| 定义方式 `definitionConfig` | `ObjectSet Definition`（对象集合定义）|
| 重算行为 / 懒加载 | 保持默认（`AUTOMATIC` / 关）|
| 界面配置 / 路由配置 / 状态保存 | 保持 `off` |

在 `ObjectSet Definition` 子表单里：

| 字段 | 填法 |
|---|---|
| 对象类型 `objectType` | 选 §3 建好的对应对象类型 |
| 过滤条件 `filters` | **留空**（留空 = 全表）；要试过滤可填 `[{"property":"is_paid","operator":"=","value":1}]` |
| 叠加过滤来源 `filterVar` | 留空 |

> ⚠️ 变量名带 `Validator.identity`（只允许 `[A-Za-z0-9_-]`），落盘时经 `normalizeName`
> 统一转小写，且**它同时是变量配置文件名**——创建后不能改（改了等于换一个变量，
> widget 里按名字引用的地方会全部指空）。三个变量名都用 ASCII，规避这个坑。

落盘位置：`{pluginCfgRoot}/ontology/test/workshop/{module}/{name}.xml`。

---

## 5. ChartXY 配置：逐表逐画法

### 5.0 通用部分（三张表一样）

| 字段 | 值 | 说明 |
|---|---|---|
| X 轴 `xAxis` | `Categorical Axis` | 只能选分类轴，选了连续轴保存时会被 `axisKindViolation()` 拦下 |
| Y 轴 `yAxis` | `Continuous Axis` | 只能选连续轴 |
| 图例 `showLegend` | `Legend` → 位置 `BOTTOM` | 选 `off` 也能出图 |
| 图表方向 `orientation` | `VERTICAL` | **只要有一层是折线/散点就只能是纵向**，保存期校验 |
| 选择写回变量 `selectionAsFilterVar` | 留空 | 要测刷选联动才填，需要先建「对象集合过滤器」变量 |

图层的「数据源变量」下拉**只列对象集合定义型变量**——由行点击写入的 ExternalInput 型变量
不会出现在这里（它们没有 `executeAggregation` 语义，选中会直接抛错）。

### 5.1 `chartxy_order_fact` —— 柱状 / 折线 / 散点 全覆盖

> 本节所有用例共用同一个对象类型与数据源变量，切换画法时只需改图层的对应字段，
> 不必重建变量。A~C 共用一套 `region` 分类轴配置，D~G 共用 `order_time`/`order_date` 时间轴配置。

对象类型 `chartxy_order_fact`，数据源变量 `order_fact_set`。

#### A. 柱状图：各大区订单金额（分类分桶 + 单系列）

图层：

| 字段 | 值 |
|---|---|
| 图层标题 | `各大区订单金额` |
| 数据源变量 | `order_fact_set` |
| 图层画法 | `Bar` |
| X 轴属性 | `region` |
| X 轴分桶 | `Categorical Bucketing` → `TOP_VALUES` / 上限 `1000` |
| 系列[0].系列名称 | `订单金额` |
| 系列[0].聚合方式 | `Sum` → 度量属性 `amount` |

X 轴排序用默认 `KEY_ASCENDING`，**实际顺序按标签的 Unicode 排**，所以是
`华东 → 华北 → 华南 → 西南`（`东`4E1C < `北`5317 < `南`5357 < `西`897F）。

期望 `bizresult`：

```json
{"layers":[{"title":"各大区订单金额","type":"bar","truncated":false,
  "series":[{"name":"订单金额","points":[
    {"x":"华东","y":141508.0},
    {"x":"华北","y":171421.2},
    {"x":"华南","y":262531.2},
    {"x":"西南","y":219334.4}
  ]}]}]}
```

> 不分段时 `segment` 为 null；fastjson 默认不序列化 null 值，所以在响应里通常**看不到**
> 这个键（而不是看到 `"segment": null`）。断言时按「缺失或 null」处理。

#### B. 柱状图：各大区 × 渠道（二次分段 + 堆叠）

在 A 上给系列[0] 增加 `分段属性 = channel`、`分段显示方式 = STACKED`。
期望 12 个点（4 区 × 3 渠道），每个 x 上 3 个 segment：

| x | 分销 | 线上 | 门店 |
|---|---|---|---|
| 华东 | 56918.00 | 38214.00 | 46376.00 |
| 华北 | 64978.20 | 48288.60 | 58154.40 |
| 华南 | 95332.70 | 79458.10 | 87740.40 |
| 西南 | 82206.40 | 64123.20 | 73004.80 |

`segment` 的遍历顺序是**字典序**：`分销(5206) → 线上(7EBF) → 门店(95E8)`。

把 `分段显示方式` 改成 `PERCENTAGE`，上述 12 个值会按「同一 x 上三个段之和 = 100」归一，
例如华东变成 `40.22 / 27.01 / 32.77`（验证「每个 x 上三段之和≈100」即可）。

#### C. 柱状图（计数）：验证「平衡数据」陷阱

同 A，但聚合方式改 `Count`、不填度量属性。
期望**四个柱子完全相同，都是 108**。这不是 bug，是 §2 第 1 条说的平衡造数。

再验证「NULL 影响聚合」：

| 系列聚合 | 度量属性 | 期望（顺序 `华东 / 华北 / 华南 / 西南`）|
|---|---|---|
| `Sum` | `cost` | 合计 `384687.26`（只累加 324 行，不是 432） |
| `Average` | `cost` | `808.86 / 1011.83 / 1617.37 / 1331.80` |
| `Approx Unique Count` | `customer_id` | `108 / 108 / 108 / 108`（各区去重后都 108，但**全表是 257**，说明客户跨区重叠） |

#### D. 折线图：按月看订单金额（`DATE` 属性 + MONTH 粒度）

| 字段 | 值 |
|---|---|
| 图层标题 | `月度订单金额` |
| 图层画法 | `Line` → 面积选项 `LINE`、显示数值标签 关 |
| X 轴属性 | `order_date`（**`DATE` 列**）|
| X 轴分桶 | `Temporal Bucketing` → 粒度 `MONTH`、单位数 `1` |
| 系列[0] | `订单金额` / `Sum` / `amount` |

期望 12 个桶，x 是 `yyyy-MM-dd'T'HH:mm:ss` 格式的桶起点：

| x | y |
|---|---|
| `2026-01-01T00:00:00` | 61970.30 |
| `2026-02-01T00:00:00` | 69242.50 |
| `2026-03-01T00:00:00` | 70537.70 |
| `2026-04-01T00:00:00` | 61213.90 |
| `2026-05-01T00:00:00` | 66269.10 |
| `2026-06-01T00:00:00` | 67679.30 |
| `2026-07-01T00:00:00` | 64124.50 |
| `2026-08-01T00:00:00` | 70601.70 |
| `2026-09-01T00:00:00` | 65795.90 |
| `2026-10-01T00:00:00` | 60388.10 |
| `2026-11-01T00:00:00` | 70130.30 |
| `2026-12-01T00:00:00` | 66841.50 |

时间桶**永远按时间升序**，不受 X 轴排序方式影响。

#### E. 折线图：按天 × 渠道（时间分桶 + 分段）

X 轴属性 `order_time`，粒度 `DAY`、单位数 `1`，系列 `Sum(amount)` + `分段属性 channel`。

期望 36 个桶（12 个月 × 3 天），每桶 3 个 segment。抽查：

| x（桶起点） | 分销 | 线上 | 门店 |
|---|---|---|---|
| `2026-01-05T00:00:00` | 8308.10 | 6649.20 | 5131.40 |
| `2026-01-15T00:00:00` | 6501.40 | 7859.50 | 6534.20 |
| `2026-12-25T00:00:00` | 9105.75 | 7254.35 | 5832.80 |

首桶 `2026-01-05` 的合计是 `20088.70`，末桶 `2026-12-25` 是 `22192.90`。

#### F. 折线图：按周（粒度单位数 = 7）

X 轴属性 `order_date`，粒度 `DAY`、**单位数 `7`**。期望 36 个桶（12 个月 × 每月 3 个有数据的
七天气窗）。桶起点是从 `1970-01-01` 起每 7 天对齐一次（**不是**自然周对齐，`DAY` 的单位数
只是把 N 天并成一个刻度），所以只有 5/15/25 三天有数据时，同一月的三个桶起点会落在
`01 / 15 / 22` 这类不等距的日期上：

| x | Sum(amount) |
|---|---|
| `2026-01-01T00:00:00` | 20088.70 |
| `2026-01-15T00:00:00` | 20895.10 |
| `2026-01-22T00:00:00` | 20986.50 |
| `2026-12-24T00:00:00` | 22192.90 |

> 想按自然周对齐要用粒度 `WEEK`。`WEEK` 的桶起点 = `1970-01-05`（周一）起算 + N 周，
> 与 `DAY + unit=7` 是两回事。

#### G. 折线图：按小时（`DATETIME` 属性 + HOUR 粒度）

改用 `order_time` + 粒度 `HOUR`、单位数 `1`。期望 **216 个桶**（36 天 × 6 个有数据的小时）。
抽查：

| x | Sum(amount) |
|---|---|
| `2026-01-05T09:00:00` | 4876.50 |
| `2026-01-05T11:00:00` | 4207.00 |
| `2026-01-05T13:00:00` | 2086.00 |

加 `分段属性 channel` 后，`2026-01-05T09:00` 只有 `线上 3705.30 / 门店 1171.20`（当天该小时没有分销单），
`2026-01-05T11:00` 是 `分销 1053.40 / 线上 792.00 / 门店 2361.60`。
**这就是验证「`nullDisplay` 缺口」的样本**：缺 segment 的桶在折线图上是缺口，柱状图是该段缺一根柱。

#### H. 散点图

散点图在 TIS 里**不是「X、Y 都是数值」**，它和柱/线共用同一套「分类 X 轴 × 聚合值 Y」的语义
（理由见同目录 `ChartXYWidget.md` 的「散点图同样是分类 X 轴」一节）。所以配置方式和柱状图一样，
只把画法换成 `Scatter`：

| 字段 | 值 |
|---|---|
| 图层画法 | `Scatter`（零专有字段：既没有面积选项也没有数值标签）|
| X 轴属性 | `product_line` |
| X 轴分桶 | `Categorical Bucketing` |
| 系列[0] | `平均单价` / `Average` / `unit_price` |

期望（按 `KEY_ASCENDING`：专业版 < 旗舰版 < 标准版）：

| x | Average(unit_price) |
|---|---|
| `专业版` | 175.35 |
| `旗舰版` | 175.35 |
| `标准版` | 175.35 |

> 三个值完全相同是造数决定的（单价只随 `region`/`channel` 变化，与 `product_line` 无关）。
> 想看到分散的点，把 X 轴换成 `region`、Y 轴用 `Average(cost)`：
> `华东 808.86 / 华北 1011.83 / 华南 1617.37 / 西南 1331.80`。

### 5.2 `chartxy_server_metric` —— 时序折线主战场

对象类型 `chartxy_server_metric`，数据源变量 `server_metric_set`。
数据范围：`2026-09-01 00:00` ~ `2026-09-07 23:00`，整点对齐，每天 24 点 × 3 台主机。

#### A. 折线图：每小时 QPS 总量

| 字段 | 值 |
|---|---|
| 图层标题 | `每小时QPS` |
| 图层画法 | `Line` |
| X 轴属性 | `event_time` |
| X 轴分桶 | 粒度 `HOUR`、单位数 `1` |
| 系列[0] | `QPS` / `Sum` / `qps` |

期望 **168 个桶**（7 × 24）。抽查：

| x | Sum(qps) |
|---|---|
| `2026-09-01T00:00:00` | 5316 |
| `2026-09-01T01:00:00` | 5265 |
| `2026-09-07T22:00:00` | 3096 |
| `2026-09-07T23:00:00` | 3045 |

#### B. 折线图：每小时 CPU × 主机（分段）

同 A，X 轴不变，系列改成 `平均CPU` / `Average` / `cpu_usage`，`分段属性 = host`。

| x | host-a | host-b | host-c |
|---|---|---|---|
| `2026-09-01T00:00:00` | 56.00 | 64.00 | 72.00 |
| `2026-09-01T01:00:00` | 56.92 | 59.92 | 67.92 |
| `2026-09-07T23:00:00` | 54.92 | 62.92 | 70.92 |

#### C. 折线图：6 小时粒度（单位数 = 6）

X 轴分桶粒度 `HOUR`、**单位数 `6`**。期望 **28 个桶**（7 天 × 4 个六小时段），
桶起点是 `00:00 / 06:00 / 12:00 / 18:00`：

| x | host-a | host-b | host-c |
|---|---|---|---|
| `2026-09-01T00:00:00` | 51.63 | 58.79 | 65.96 |
| `2026-09-01T06:00:00` | 39.63 | 45.96 | 53.13 |
| `2026-09-07T18:00:00` | 49.71 | 56.88 | 64.04 |

每个桶的 `COUNT(*)` 都是 6（6 小时 × 1 台机 = 6 行），可以顺手确认没有串桶。

#### D. 折线图：按天 QPS 总量

粒度 `DAY`。期望 **7 个桶**：

| x | Sum(qps) |
|---|---|
| `2026-09-01T00:00:00` | 101064 |
| `2026-09-02T00:00:00` | 101160 |
| `2026-09-03T00:00:00` | 101256 |
| `2026-09-04T00:00:00` | 101352 |
| `2026-09-05T00:00:00` | 101040 |
| `2026-09-06T00:00:00` | 101136 |
| `2026-09-07T00:00:00` | 101232 |

#### E. 柱状图：各主机累计错误数

| 字段 | 值 |
|---|---|
| 图层画法 | `Bar` |
| X 轴属性 | `host` |
| X 轴分桶 | `Categorical Bucketing` |
| 系列[0] | `错误数` / `Sum` / `err_count` |

期望：`host-a 672 / host-b 840 / host-c 504`。

#### F. 散点图：各主机峰值 CPU

画法 `Scatter`，X 轴 `host`，系列 `峰值CPU` / `Max` / `cpu_usage`。
期望：`host-a 61.00 / host-b 68.00 / host-c 76.00`。

#### G. 按机房分段（2 值低基数）

X 轴 `host`、系列 `Average(cpu_usage)`、`分段属性 = region`。
`region` 只有两个值且与主机绑定：`上海机房=host-c`、`北京机房=host-a,host-b`。
期望：

| x | 上海机房 | 北京机房 |
|---|---|---|
| `host-a` | — | 46.22 |
| `host-b` | — | 53.21 |
| `host-c` | 60.23 | — |

（`segment` 缺失的点不返回，正是验证「缺段」渲染的样本。）

### 5.3 `chartxy_daily_report` —— `DATE` 分桶边界

对象类型 `chartxy_daily_report`，数据源变量 `daily_report_set`。
数据范围：`2026-07-01` ~ `2026-09-29`，91 天 × 3 部门（数据平台 / 风控 / 结算）。

#### A. 柱状图：各部门失败任务数

| 字段 | 值 |
|---|---|
| 图层画法 | `Bar` |
| X 轴属性 | `department` |
| X 轴分桶 | `Categorical Bucketing` |
| 系列[0] | `失败数` / `Sum` / `task_failed` |

期望（`KEY_ASCENDING`：数据平台(6570) < 结算(7ED3) < 风控(98CE)）：
`数据平台 501 / 结算 499 / 风控 500`。

#### B. 柱状图：各部门平均耗时（含 NULL）

系列改 `Average` / `duration_sec`。期望：`数据平台 22.29 / 结算 22.56 / 风控 22.24`
（分母不含 NULL 行：`COUNT(duration_sec)=249`，不是 273）。

#### C. 折线图：按天看任务总量

X 轴属性 `report_date`、粒度 `DAY`、单位数 `1`，系列 `Sum(task_total)`。
期望 **91 个桶**。抽查：

| x | Sum(task_total) |
|---|---|
| `2026-07-01T00:00:00` | 411 |
| `2026-07-02T00:00:00` | 420 |
| `2026-07-03T00:00:00` | 429 |
| `2026-09-28T00:00:00` | 672 |
| `2026-09-29T00:00:00` | 681 |

加 `分段属性 = department` 后，`2026-07-01` 是 `数据平台 120 / 结算 154 / 风控 137`。

#### D. 折线图：WEEK 粒度（自然周）

粒度 `WEEK`、单位数 `1` → **14 个桶**；单位数 `2` → **8 个桶**。
`unitValue=2` 的桶起点与行数：

| x | 行数 | AVG(duration_sec) |
|---|---|---|
| `2026-06-22T00:00:00` | 15 | 19.51 |
| `2026-07-06T00:00:00` | 42 | 21.74 |
| `2026-07-20T00:00:00` | 42 | 22.31 |
| `2026-08-03T00:00:00` | 42 | 22.72 |
| `2026-08-17T00:00:00` | 42 | 23.39 |
| `2026-08-31T00:00:00` | 42 | 21.78 |
| `2026-09-14T00:00:00` | 42 | 22.35 |
| `2026-09-28T00:00:00` | 6 | 28.25 |

⚠️ 首桶只有 15 行、末桶只有 6 行 —— 这是**正确**的：周桶按自然周切，数据只覆盖 07-01~09-29。
若首桶不是 15 行，说明分桶没按自然周对齐。

#### E. 折线图：MONTH / QUARTER / YEAR

| 粒度 | 桶数 | 期望 |
|---|---|---|
| `MONTH` | 3 | `2026-07-01T00:00:00` → `SUM(task_failed)=510 / SUM(task_total)=16926 / AVG(dur)=21.96`；`2026-08-01T00:00:00` → `507 / 21615 / 22.30`；`2026-09-01T00:00:00` → `483 / 16095 / 22.87` |
| `QUARTER` | 1 | `2026-07-01T00:00:00` → `SUM(task_total)=54636`，`COUNT(*)=273` |
| `YEAR` | 1 | `2026-01-01T00:00:00` → `SUM(task_total)=54636` |

> QUARTER 与 YEAR 在这里都是 1 个桶，因为数据全在 2026 Q3。要验证季度切分，请用
> `chartxy_order_fact.order_date` + `QUARTER`：4 个桶
> `2026-01-01T00:00:00 → 201750.50`、`2026-04-01T00:00:00 → 195162.30`、
> `2026-07-01T00:00:00 → 200522.10`、`2026-10-01T00:00:00 → 197359.90`。

---

## 6. 多图层与多形状

### 6.1 组合图（柱 + 线）

在同一张图里配两个图层，分别指向 `order_fact_set`：

| 图层 | 画法 | X 轴属性 | 系列 |
|---|---|---|---|
| `订单量` | `Bar` | `region` | `Count`（无度量属性）|
| `平均客单价` | `Line` | `region` | `Average` / `amount` |

两个图层各自独立取数、独立成 series，前端按 `layers[i].type` 分别画柱和线。
**方向必须是 `VERTICAL`**——有折线层时选横向保存会被拦（见 §7）。

### 6.2 一个图层两个系列、`segmentBy` 不同 → 两条 SQL

`AggregationPushdownPlanner.plan` 先按 `segmentBy` 把系列分组成 shape，
**每个 shape 发一条 SQL**：

| 系列 | 聚合 | segmentBy |
|---|---|---|
| `各渠道金额` | `Sum(amount)` | `channel` |
| `总金额` | `Sum(amount)` | （空）|

这会产生两条 SQL：一条 `group by c0, c1`（region, channel），一条 `group by c0`（region）。
返回结果在内存里按 x 合并成两个 series。验证方式：开 MySQL 的 `general_log`，
或用数据源上的慢查询/连接日志，确认语句条数 = 2。

如果两个系列的 `segmentBy` **相同**，只发一条 SQL（同一 shape 内多指标）。

---

## 7. 负例：必须被拦下的配置

这些用例的目的是确认**校验真的生效**，且报错文案可定位问题。
错误文案来自后端抛出的异常原文（`AggregationPushdownPlanner` / `ChartXYDataProcessor` /
`ChartXYWidget`），`get_data` 会把它放进响应的失败信息里。

| # | 配置 | 期望报错 |
|---|---|---|
| 1 | `report_date`（`DATE`）+ 粒度 `HOUR` | `ChartXY x property:'report_date' is a DATE (no time part), granularity:HOUR would put every row into a single bucket; use TIMESTAMP or a coarser granularity` |
| 2 | `region`（`STRING`）+ `Temporal Bucketing` | `ChartXY x property:'region' type:STRING can not use temporal bucketing, only DATE/TIMESTAMP support it` |
| 3 | 系列 `Sum` + 度量属性 `region`（`STRING`） | `ChartXY series '订单金额' aggregation:SUM requires a numeric property, but property:'region' type is:STRING` |
| 4 | X 轴属性填一个不存在的列 `foo` | `xAxisProperty column:'foo' does not exist in object set properties:[order_id, order_time, ...]` |
| 5 | X 轴属性填 `bad-name!` | `xAxisProperty column:'bad-name!' is not a legal db column name, must match (^_([a-zA-Z0-9]_?)*$)\|(^[a-zA-Z0-9](_?[a-zA-Z0-9])*_?$)` |
| 6 | X 轴属性留空 | `xAxisProperty can not be blank` |
| 7 | 系列 `Sum` 但度量属性留空 | `ChartXY layer '月度订单金额' series '订单金额' aggregation:SumAggregation requires a metric property, but it is blank` |
| 8 | X 轴选 `Continuous Axis` | `X 轴必须是分类轴：X 上的取值是各图层按「X 轴属性」分组得到的离散标签（分类键或时间桶），不是原始数值。散点图这类需要数值 X 轴的场景暂未支持。` |
| 9 | Y 轴选 `Categorical Axis` | `Y 轴必须是连续轴：Y 上绘制的是各系列的聚合结果（count / sum / avg 等数值），分类轴在这里无法表达数值刻度。` |
| 10 | 方向 `HORIZONTAL` + 任一折线/散点图层 | `图表方向不能设为横向：图层「趋势」是折线图，只有柱状图图层支持横向。请把图表方向改为纵向，或调整该图层的画法。` |
| 11 | 图层未选数据源变量 | `ChartXY layer 'xxx' dataSourceVar can not be blank` |
| 12 | 数据源变量选了一个 ExternalInput 型变量（绕过下拉直接改 XML） | `ChartXY layer 'xxx' dataSourceVar:'yyy' is not an object set variable` |
| 13 | `segmentBy` 填一个不存在的列 | `segmentBy column:'foo' does not exist in object set properties:[...]` |
| 14 | 静态过滤条件引用不存在的列 | `filter property column:'foo' does not exist in object set properties:[...]` |

补充说明：

- 第 1 条是**故意**的：`DATE` 没有时间部分，切小时等于把所有行塞进一个桶。
- 第 3 条文案里引号中的 `'订单金额'` 是**系列名**（`series.name`），`property:'region'` 才是
  属性名；第 1/2 条的 `property:'...'` 也都是属性名。这是后端文案的真实差异，别当成笔误。
- 第 8/9 条发生在**保存期**（`DescriptorImpl#verify`）与**运行期**（`ChartXYDataProcessor#process`）
  两处，绕过表单直接 POST 一样会被拦。

`dataSourceVar` 下拉里**不应出现** ExternalInput 型变量 —— 这是 `getObjectSetSourceVariableOptions`
与旧的 `getObjectSetVariableOptions` 的差别（后者会把它们列进来，选中必炸）。

---

## 8. 已知边界（测出来不算 bug）

1. **`CategoricalBucketing.mode` 目前对 SQL 没有影响。** `TOP_VALUES` 与 `EXACT_VALUES`
   只在 `CategoricalBucketing` 内部与前端类型定义里出现，pushdown 链路（`AggregationPushdownPlanner`
   → `DataSourceBinding.executeAggregation`）不读它。**真正生效的是 `limit`。**
2. **X 轴的 `limit` 是内存侧截断，不会置 `truncated=true`。** `truncated` 只反映
   `AggregationCriteria.maxGroups`（`DEFAULT_MAX_GROUPS = 50000`）有没有被突破。
   把 `limit` 调到 2 会发现桶少了、但 `truncated` 仍是 `false` —— 这是当前实现，不要当成
   「截断提示失效」去改（真要改得先定语义）。
3. **`segmentBy` 不做基数预检。** 拿高基数列（如 `order_id`）当 `segmentBy` 会按
   「x 桶 × segment」展开成上万段，唯一的兜底是同一个 `maxGroups`。
4. **`nullDisplay` / `segmentOverride` 是前端渲染语义**，后端只按枚举回传，不参与计算。
   验证它们要去前端看图形，不能只看 `bizresult`。
5. **`chartxy_order_fact` 的 Count 图各柱等高**（见 §2 陷阱 1），这是造数特性。
6. **`SeriesConfig.variable` 已于 2026-10-27 删除。** 它曾经是 `ordinal=1` 的必填
   `SELECTABLE`，但候选恒为空、前端 SELECTABLE 分支不给手输入口、后端零读取——三者叠加使
   「添加系列」对话框永远保存不了（服务端报 `prop:variable can not be empty`）。
   数据来源本就是图层职责（`ChartLayer.dataSourceVar`）。测试时**不要去找这个字段**；
   若在旧文档/旧配置里见到它，忽略即可。

---

## 9. 一条命令核对全部预期值

不想点界面时，可以直接在库里把 §5 的期望值一次性打出来对照：

```sql
-- order_fact：按 region 的 Sum(amount)
select region, round(sum(amount),2) from chartxy_order_fact group by 1 order by 1;
-- order_fact：按 region × channel
select region, channel, round(sum(amount),2) from chartxy_order_fact group by 1,2 order by 1,2;
-- order_fact：NULL 语义
select count(*) total, count(cost) non_null, round(sum(cost),2), count(distinct customer_id) from chartxy_order_fact;
-- order_fact：按天
select FLOOR(DATEDIFF(order_time,'1970-01-01')/1)*1 c0, round(sum(amount),2)
  from chartxy_order_fact group by 1 order by 1;
-- server_metric：按小时
select FLOOR(TIMESTAMPDIFF(SECOND,'1970-01-01 00:00:00',event_time)/(3600*1))*1 c0, sum(qps)
  from chartxy_server_metric group by 1 order by 1;
-- daily_report：按自然周（unitValue=2）
select FLOOR(DATEDIFF(report_date,'1970-01-05')/(7*2))*2 c0, count(*) n, round(avg(duration_sec),2)
  from chartxy_daily_report group by 1 order by 1;
```

这六条 SQL 的形态与 `DataSourceBinding.executeAggregation` 实际发出的语句**同构**
（分组键表达式 + 指标 + `GROUP BY 序号` + `LIMIT maxGroups+1`），因此数值可以直接对齐
`bizresult.layers[*].series[*].points[*].y`。
