## tabEntities

导入流实体名称，目标端以此作为表名，多个表以逗号分隔。支持wildcard样式
，当消息体中表名为 `order_detail_001`,`order_detail_002`,`order_detail_003` 则可以用 wildcard表达式`order_detail*`匹配，最终将多个匹配的物理表名归并成`order_detail`逻辑表名