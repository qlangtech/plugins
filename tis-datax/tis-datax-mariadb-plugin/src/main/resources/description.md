封装MariaDB作为数据源的DataSource插件，可以向TIS导入MySQL中的数据表作后续分析处理
* 封装`MariaDB`数据源驱动

  驱动版本为[MariaDB Connector/J is for Java 8:3.4.1](https://mariadb.com/downloads/connectors/connectors-data-access/java8-connector), 支持`MariaDB`数据源以JDBC的方式连接

* MariaReader

  MariaReader插件实现了从MariaDB读取数据。在底层实现上，MariaReader通过JDBC连接远程MariaDB数据库，并执行相应的sql语句将数据从MariaDB库中SELECT出来
  
* MariaWriter

  实现了Alibaba MariaWriter 插件，写入数据到 MariaDB 主库的目的表的功能。在底层实现上， MariaWriter 通过 JDBC 连接远程 MariaDB 数据库，并执行相应的 insert into ... 或者 ( replace into ...) 的 sql 语句将数据写入 MariaDB，内部会分批次提交入库。