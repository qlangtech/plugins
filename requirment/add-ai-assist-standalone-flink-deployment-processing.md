# 需求说明
当TIS AI agent部署实时增量通道过程中，需要使用Standalone模式部署实时增量通道过程中，发现本地环境中还没有可用Standalone Flink Cluster
，需要在Agent执行流程执行自动部署Standalone模式的Flink Cluster

# 执行流程说明

部署自动部署Standalone模式的Flink Cluster分为以下几个步骤
1.  下载flink Standalone压缩包，
2.  解压下载的flink Standalone压缩包，（是否可以让下载和解压直接在内存中实现？不需要将压缩包在磁盘中临时保存），下载前需要校验StandaloneFlinkDeployingAIAssistSupport.this.flinkDeployDir指定的目录是否存在及是否有权限
3.  修改Flink的配置文件（flink-tis-1.20.1/conf/config.yaml，本地环境中我已经存放了一份，路径在/Users/mozhenghua/Downloads/flink-tis-1.20.1-bin (1)/conf/config.yaml），修改环境变量，需要修改的环境变量如下：
    1. 在jobmanager和taskmanager的启动java option配置中添加： -Ddata.dir=StandaloneFlinkDeployingAIAssistSupport.this.dataDir, "-D"+CenterResource.KEY_notFetchFromCenterRepository=true
    2. 修改taskmanager的启动`numberOfTaskSlots` 配置项为StandaloneFlinkDeployingAIAssistSupport.this.slot
    3. 修改taskmanager的启动的内存大小设置为，StandaloneFlinkDeployingAIAssistSupport.this.tmMemory ，需要加一个校验，当前机器节点是否有足够的内存，需要防止Flink 启动之后内存不足
    4. 修改Flink 暴露的的REST API Http 端口由默认的8081端口，设置成StandaloneFlinkDeployingAIAssistSupport.this.port
    
    修改Flink的配置文件，需要实现实现即使多次重复操作最终文件内容也是相同的（实现幂等）
4.  调用 Flink的启动脚本（flink-tis-1.20.1/bin/start-cluster.sh，路径在：/Users/mozhenghua/Downloads/flink-tis-1.20.1-bin (1)/bin/start-cluster.sh）启动Flink，如果不能正常启动需要报告错误信息

# 代码入口说明

已经写了一个骨架代码 StandaloneFlinkDeployingAIAssistSupport（路径为：tis-incr/tis-realtime-flink/src/main/java/com/qlangtech/plugins/incr/flink/launch/clustertype/StandaloneFlinkDeployingAIAssistSupport.java）
的startProcess() 函数