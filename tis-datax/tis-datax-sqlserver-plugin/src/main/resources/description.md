* 封装SqlServer作为数据源的DataSource插件，可以向TIS导入SqlServer中的数据表作后续分析处理
* SqlServerReader
  插件实现了从SqlServer读取数据。在底层实现上，SqlServerReader通过JDBC连接远程SqlServer数据库，并执行相应的sql语句将数据从SqlServer库中SELECT出来。[详细](https://github.com/alibaba/DataX/blob/master/sqlserverreader/doc/sqlserverreader.md)
