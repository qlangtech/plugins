## batchSize
 * 描述：一次性批量提交的记录数大小，该值可以极大减少DataX与Mysql的网络交互次数，并提升整体吞吐量。但是该值设置过大可能会造成DataX运行进程OOM情况。
## preSql
 描述：写入数据到目的表前，会先执行这里的标准语句。如果 Sql 中有你需要操作到的表名称，请使用 `@table` 表示，这样在实际执行 Sql 语句时，会对变量按照实际表名称进行替换。比如你的任务是要写入到目的端的100个同构分表(表名称为:datax_00,datax01, ... datax_98,datax_99)，并且你希望导入数据前，先对表中数据进行删除操作，那么你可以这样配置：`"preSql":["delete from 表名"]`，效果是：在执行到每个表写入数据前，会先执行对应的 delete from 对应表名称
## postSql
 写入数据到目的表后，会执行这里的标准语句。（原理同 preSql ）
## session
  DataX在获取Mysql连接时，执行session指定的SQL语句，修改当前connection session属性 

## autoCreateTable

解析Reader的元数据，自动生成Writer create table DDL语句，有三种选择：
* `off`：关闭自动生成及同步目标端建表DDL语句，当目标端表实例已经存在可选择此选项。
* `default`：打开动生成及自动执行目标端建表DDL语句，执行任务状态由程序自动控制毋需人为干涉。
* `customized`：用户可自定义设置`自动执行目标端建表DDL语句逻辑`，如：是否需要生成列注释等。