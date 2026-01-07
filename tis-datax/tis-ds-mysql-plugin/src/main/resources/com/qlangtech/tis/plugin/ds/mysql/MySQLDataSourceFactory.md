## useCompression

与服务端通信时采用zlib进行压缩，效果请参考[https://blog.csdn.net/Shadow_Light/article/details/100749537](https://blog.csdn.net/Shadow_Light/article/details/100749537)

## splitTableStrategy

数据库中使用分表策略：
* `off`：不启用分表策略
* `on`: 启用分表策略。每张分表数据结构需要保证相同，且有规则的后缀作为物理表的分区规则，逻辑层面视为同一张表。如逻辑表`order` 对应的物理分表为：  `order_01`,`order_02`,`order_03`,`order_04`

**注意**：若用户未提及分表、多节点、表后缀等关键词，默认选择 id = 'off'

[详细说明](https://tis.pub/docs/guide/datasource/multi-table-rule/)




