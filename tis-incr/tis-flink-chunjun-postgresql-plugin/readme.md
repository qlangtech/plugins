由于flink-cdc 版本的 source connector 已经启用 chunjun 版本的 source connector先关闭了

修改../pom.xml 配置
```xml
 <module>tis-flink-chunjun-postgresql-plugin</module>
```
改为：
```xml
<!--        <module>tis-flink-chunjun-postgresql-plugin</module>-->
```
