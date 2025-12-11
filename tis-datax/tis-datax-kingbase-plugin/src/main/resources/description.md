封装`人大金仓`作为数据源的DataSource插件，可以向TIS导入`人大金仓`中的数据表作后续分析处理
* KingBaseReader

  KingBaseReader插件实现了从`人大金仓`批量读取数据。在底层实现通过JDBC连接远程`人大金仓`数据库，并执行相应的sql语句将数据从库中SELECT方式导出数据

* KingBaseWriter

  实现批量写入数据到 `人大金仓` 库的目的表的功能。在底层实现上， KingBaseWriter 通过 JDBC 连接远程 `人大金仓` 数据库，并执行相应的 insert into ... 或者 ( replace into ...) 的 sql 语句将数据写入 `人大金仓`

* 提供`人大金仓`数据源驱动
  驱动版本为`kingbase8:${kingbase.version}`, 支持`人大金仓`服务端9.X的数据源以JDBC的方式连接