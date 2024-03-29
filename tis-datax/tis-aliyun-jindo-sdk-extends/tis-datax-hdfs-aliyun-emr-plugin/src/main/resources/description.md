JindoFS 是阿里云针对云上存储定制的自研大数据存储服务，完全兼容 Hadoop 文件系统接口，给客户带来更加灵活、高效的计算存储方案，目前已验证支持阿里云 EMR 中所有的计算服务和引擎：Spark、Flink、Hive、MapReduce、Presto、Impala 等。JindoFS 提供了块存储模式（Block）和缓存模式（Cache）的存储模式。
* 提供基于Aliyun JindoData 的HDFS 实现
  
  本插件提供基于 [jindosdk 4.6.11](https://github.com/aliyun/alibabacloud-jindodata/blob/master/docs/user/4.x/4.6.x/4.6.11/jindofs/hadoop/jindosdk_ide_hadoop.md) 封装实现，用户如需要向阿里云 EMR HDFS中写入数据
  可选择本插件。
  
* JindoFS 存储系统

  基于阿里云 OSS 的云原生存储系统，二进制兼容 Apache HDFS，并且基本功能对齐，提供优化的 HDFS 使用和平迁体验。是原 JindoFS Block 模式的全新升级版本。 阿里云 OSS-HDFS 服务（JindoFS 服务) 是 JindoFS 存储系统在阿里云上的服务化部署形态，和阿里云 OSS 深度融合，开箱即用，无须在自建集群部署维护 JindoFS，免运维。OSS-HDFS 服务具体介绍请参考 [OSS-HDFS服务概述](https://help.aliyun.com/zh/oss/user-guide/overview-1) 。

