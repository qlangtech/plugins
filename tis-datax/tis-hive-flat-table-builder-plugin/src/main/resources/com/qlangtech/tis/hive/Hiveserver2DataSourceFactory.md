## alternativeHdfsSubPath

假设当前数据库名称为 `dezhou`，有一张名称为 `test001`的表，默认hdfs上的路径为:

`hdfs://10.8.0.10:9000/user/hive/warehouse/dezhou/test001` 

而用户的应用场景需要为：

`hdfs://10.8.0.10:9000/user/hive/warehouse/dezhou.db/test001`

此时需要设置此属性为：**$1.db**，当然也可以直接设置为：**dezhou.db**（只不过当dbName变化，此属性不会随之变化）