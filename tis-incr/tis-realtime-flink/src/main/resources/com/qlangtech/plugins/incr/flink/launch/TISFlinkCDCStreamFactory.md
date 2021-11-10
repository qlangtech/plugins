## flinkCluster 

对应Flink的执行任务集群，TIS组装好Flink Job之后，提交任务时会向 Flink Cluster中提交任务。

TIS平台中提交Flink任务之前，请先创建Flink Cluster，支持两种模式：

1. Native Kubernetes: [详细请查看](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/deployment/resource-providers/native_kubernetes/)
2. Standalone: [详细请查看](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/deployment/resource-providers/standalone/overview/)

## parallelism

任务执行并行度的意思。

在 Flink 里面代表每个任务的并行度，适当的提高并行度可以大大提高 job 的执行效率，比如你的 job 消费 kafka 数据过慢，适当调大可能就消费正常了。

