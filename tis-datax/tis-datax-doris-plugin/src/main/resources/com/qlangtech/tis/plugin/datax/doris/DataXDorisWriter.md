## loadProps

StreamLoad 的请求参数,默认传入的数据均会被转为字符串，并以 **\t** 作为列分隔符，**\n** 作为行分隔符，组成csv文件进行 [StreamLoad导入参数说明](http://doris.apache.org/master/zh-CN/administrator-guide/load-data/stream-load-manual.html#%E5%AF%BC%E5%85%A5%E4%BB%BB%E5%8A%A1%E5%8F%82%E6%95%B0)。 如需更改列分隔符， 则正确配置 loadProps 即可：

```json
 {
  "column_separator": "\\x01",
  "line_delimiter": "\\x02"
}
```

## createTableModel 

TIS可以帮助用户自动生成Doris端的建表DDL语句，如Doris中已存在对应的表可选择`Off`,如需要生成可以选择`Unique`和`Duplicate`之一，如需要使用`Aggregate`模型，由于Agg模型需要设置非聚合列的聚合函数，系统无法预知。
可先选择`Unique`和`Duplicate`任意一种，待到DDL生成之后，手动在DDL之上进行修改。

Doris 支持三种数据模型：

1. Aggregate
2. Unique
3. Duplicate
   
[数据模型详细](https://doris.apache.org/docs/table-design/data-model/overview)



