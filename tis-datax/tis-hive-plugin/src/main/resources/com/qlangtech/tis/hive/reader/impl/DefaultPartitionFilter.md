## ptFilter

每次触发全量读取会使用输入项目的表达式对所有分区进行匹配，默认值为 `pt=latest`，假设系统中存在两个分区路径：1. pt=20231111121159 , 2. pt=20231211121159

很明显 `pt=20231211121159` 为最新分区，会作为目标分区进行读取。

用户也可以在输入框中输入 `pt=’20231211121159‘` 强制指定特定分区作为目标分区进行读取。也可以在输入项目中使用过滤条件进行匹配，例如：`pt=’20231211121159‘ and pmod='0'`

## pathPattern
支持两种分区路径格式：
1. `WithoutPtKeys`: 分区路径上`不包含`分区字段名，如：**/user/hive/warehouse/sales_data/2023/1**
2. `WithPtKeys`:（默认值） 分区路径上`包含`分区字段名，如：**/user/hive/warehouse/sales_data/year=2023/month=1**
