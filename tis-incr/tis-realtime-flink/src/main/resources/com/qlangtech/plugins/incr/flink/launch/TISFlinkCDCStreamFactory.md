## cluster
对应Flink的执行任务集群，TIS组装好Flink Job之后，提交任务时会向 Flink Cluster中提交任务。

TIS平台中，提交任务前，请先创建Flink Cluster，其支持三种部署模式：

1. Kubernetes Session: [详细请查看](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/deployment/resource-providers/native_kubernetes/#session-mode)
   
   特点是多个Flink Job任务会由同一个Job Manager分配资源调度

2. Kubernetes Application: [详细请查看](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/deployment/resource-providers/native_kubernetes/#application-mode)
   [Application Mode Detail](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/deployment/overview/#application-mode)
   
   每个Flink Job任务独占一个JobManager ，对于运行在集群中的Job不会有资源抢占问题，
   >因此对于比较重要且优先级的任务，建议采用这种部署方式
      

3. Standalone: [详细请查看](https://nightlies.apache.org/flink/flink-docs-release-1.14/docs/deployment/resource-providers/standalone/overview/)
    
   这种部署方式最简单，用户下载TIS 定制过的Flink安装包，解压，修改配置后即可启动运行，因为是单机版的，由于单机slot资源限制只可以部署有限Flink Job任务 [安装说明](https://tis.pub/docs/install/flink-cluster/standalone)  

## parallelism

任务执行并行度

在 Flink 里面代表每个任务的并行度，适当的提高并行度可以大大提高 job 的执行效率，比如你的 job 消费 kafka 数据过慢，适当调大可能就消费正常了。

## restartStrategy

The cluster can be started with a default restart strategy which is always used when no job specific restart strategy has been defined. In case that the job is submitted with a restart strategy, this strategy overrides the cluster’s default setting.

Detailed description:[restart-strategies](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/state/task_failure_recovery/#restart-strategies)

There are 4 types of restart-strategy:

1. `off`: No restart strategy.
2. `fixed-delay`: Fixed delay restart strategy. More details can be found [here](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/state/task_failure_recovery/#fixed-delay-restart-strategy).
3. `failure-rate`: Failure rate restart strategy. More details can be found [here](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/state/task_failure_recovery#failure-rate-restart-strategy).
4. `exponential-delay`: Exponential delay restart strategy. More details can be found [here](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/state/task_failure_recovery#exponential-delay-restart-strategy).


## checkpoint

Checkpoints make state in Flink fault tolerant by allowing state and the corresponding stream positions to be recovered, thereby giving the application the same semantics as a failure-free execution.

Detailed description:
1. [https://nightlies.apache.org/flink/flink-docs-master/docs/ops/state/checkpoints/](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/state/checkpoints/)
2. [https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/fault-tolerance/checkpointing/](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/fault-tolerance/checkpointing/)

## stateBackend

Flink provides different state backends that specify how and where state is stored.

State can be located on Java’s heap or off-heap. Depending on your state backend, Flink can also manage the state for the application, meaning Flink deals with the memory management (possibly spilling to disk if necessary) to allow applications to hold very large state. By default, the configuration file flink-conf.yaml determines the state backend for all Flink jobs.

However, the default state backend can be overridden on a per-job basis, as shown below.

For more information about the available state backends, their advantages, limitations, and configuration parameters see the corresponding section in [Deployment & Operations](https://nightlies.apache.org/flink/flink-docs-master/docs/ops/state/state_backends/).

## enableRestore

支持任务恢复，当Flink节点因为服务器意外宕机导致当前运行的flink job意外终止，需要支持Flink Job恢复执行，

需要Flink配置支持：

1. 持久化stateBackend
2. 开启checkpoint

## rateLimiter

用于 **限流读取数据** 的抽象实现，其主要目的是控制数据源的读取速率，避免下游系统过载或资源耗尽。

**一、主要目的**
1. **流量控制**
   - 限制从数据源（如 Kafka、数据库、文件等）读取数据的速率，避免突发流量压垮下游算子或外部系统。
   - 防止反压（Backpressure）向上游传递，导致源端压力过大（如 Kafka 消费者组延迟激增）。

2. **资源保护**
   - 避免因数据消费过快导致内存溢出（OOM）或 CPU/网络带宽被占满。
   - 在批流混合场景中，平衡批处理任务的资源占用。

3. **稳定性与公平性**
   - 在多租户或共享集群中，确保不同作业公平使用资源。
   - 模拟生产环境的真实流量速率（如压测或数据回放场景）。





