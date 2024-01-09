## flinkCluster 

对应Flink的执行任务集群，TIS组装好Flink Job之后，提交任务时会向 Flink Cluster中提交任务。

TIS平台中提交Flink任务之前，请先创建Flink Cluster，支持两种模式：

1. Native Kubernetes: [详细请查看](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/deployment/resource-providers/native_kubernetes/)
   [安装说明](http://tis.pub/docs/install/flink-cluster/)：
      - 在本地局域网中安装k8s环境
      - 在TIS中部署Flink-Cluster，[入口](/base/flink-cluster)

2. Standalone: [详细请查看](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/deployment/resource-providers/standalone/overview/)
   
   [安装说明](http://tis.pub/docs/install/flink-cluster/standalone/):
      - 下载、解压
        ```shell script
         wget http://tis-release.oss-cn-beijing.aliyuncs.com/${project.version}/tis/flink-tis-1.13.1-bin.tar.gz && rm -rf flink-tis-1.13.1 && mkdir flink-tis-1.13.1 && tar xvf flink-tis-1.13.1-bin.tar.gz -C ./flink-tis-1.13.1
        ```
      - 启动Flink-Cluster：
         ```shell script
         ./bin/start-cluster.sh
         ```
