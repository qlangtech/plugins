## flinkCluster 

Standalone 集群: [详细请查看](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/deployment/resource-providers/standalone/overview/)
   
[安装说明](http://tis.pub/docs/install/flink-cluster/standalone/):
1. 下载、解压

   ```shell script
     wget http://tis-release.oss-cn-beijing.aliyuncs.com/4.3.0-SNAPSHOT/tis/flink-tis-1.18.1-bin.tar.gz && rm -rf flink-tis-1.18.1 && mkdir flink-tis-1.18.1 && tar xvf flink-tis-1.18.1-bin.tar.gz -C ./flink-tis-1.18.1
   ```
2. 修改 `$FLINK_HOME/conf/flink-conf.yaml`

   ```yaml
   # The address that the REST & web server binds to
   # By default, this is localhost, which prevents the REST & web server from
   # being able to communicate outside of the machine/container it is running on.
   #
   # To enable this, set the bind address to one that has access to outside-facing
   # network interface, such as 0.0.0.0.
   #
   rest.bind-address: 0.0.0.0
    ```
   这样使Flink启动之后，可以从其他机器节点访问flink所在的节点
   
   ```yaml
   # The number of task slots that each TaskManager offers. Each slot runs one parallel pipeline.
   taskmanager.numberOfTaskSlots: 1
   ```
   默认值是1，需要在单个Flink节点上运行多个Flink任务，可修改成大于1的值就行（一般情况slot代表了服务节点的资源并行处理能力，一般配置于节点CPU核数相一致即可）
   
3. 启动Flink-Cluster：
   ```shell script
    ./bin/start-cluster.sh
   ```
