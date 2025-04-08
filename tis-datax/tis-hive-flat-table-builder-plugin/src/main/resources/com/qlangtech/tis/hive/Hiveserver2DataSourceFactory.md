## alternativeHdfsSubPath

假设当前数据库名称为 `dezhou`，有一张名称为 `test001`的表，默认hdfs上的路径为:

`hdfs://10.8.0.10:9000/user/hive/warehouse/dezhou/test001` 

而用户的应用场景需要为：

`hdfs://10.8.0.10:9000/user/hive/warehouse/dezhou.db/test001`

此时需要设置此属性为：`$1.db` ，当然也可以直接设置为： `dezhou.db`（只不过当dbName变化，此属性不会随之变化）

## hms

是HiveServer2（HS2）的默认服务，其核心作用是为远程客户端提供与Hive交互，允许远程查询执行：

1. 允许客户端（如JDBC、ODBC、Beeline等）通过TCP/IP远程提交HiveQL查询（如SELECT、JOIN等），而无需直接访问Hadoop集群节点。
2. 将HiveQL转换为底层计算任务（如MapReduce、Tez、Spark）并提交到集群执行。

## metadata

是Hive Metastore服务，其核心作用是管理Hive的元数据（Metadata）。

**Hive Metastore 的作用**
1. **元数据存储**：
    - 存储所有Hive表、分区、列、数据类型、存储位置（如HDFS路径）等元数据信息。
    - 记录表的创建时间、所有者、权限等管理信息。

2. **元数据共享**：
    - 允许多个Hive实例（或计算引擎如Spark、Impala）**共享同一份元数据**，避免重复定义表结构。
    - 例如：Spark可以直接通过Hive Metastore读取表结构，无需重新定义表。

3. **解耦元数据与计算**：
    - Metastore服务独立于HiveServer2，使得元数据管理（9083端口）与查询执行（10000端口）分离，提高系统灵活性和可维护性。

4. **支持多种后端存储**：
    - 默认使用**关系型数据库**（如MySQL、PostgreSQL）持久化元数据（而非HDFS），确保元数据的高效查询和事务支持。