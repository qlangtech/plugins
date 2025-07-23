## resMatcher

扫描指定`资源`属性下的有效资源文件，作为TDFS的资源进行按行迭代读取。根据扫描策略不同，可分为以下几种方式，用户可以按照自己需要进行选择。

* ByMeta
  
  使用TIS 的`TDFS Writer`类型，在写出过程中开启向目标文件中写入元数据信息。使用`TDFS Reader` 读取 DFS文件，只需要选择`ByMeta`选项，TIS就会先
  自动到目标目录中去获取元数据信息（元数据信息包括，字段名称，字段JDBC type类型信息，是否为主键等）。
  
  利用自动生成的元数据信息，可免去手动设置字段名称，字段类型等流程，从而大幅提高工作效率。
  例如：使用`TDFS Writer`将 MySQL库中的数据导入到 hdfs中，目录资源分布如下：
  
  ```shell script
  /user/admin/order2/instancedetail/meta.json
  /user/admin/order2/instancedetail/instancedetail__963eec63_8934_458b_bd7d_0040db533c93.csv
  /user/admin/order2/instancedetail/instancedetail__e59be359_39b7_4297_9244_90b72762e239.csv
  ```
  
  在资源选项的`path`属性设置为`/user/admin/order2`,TIS会遍历子目录读取`meta.json`获得instancedetail表对应的元数据信息
  
* Wildcard

  使用`wildcard`表达式，扫描目标目录下与`wildcard`表达式匹配的目标资源，进行遍历读取



  
