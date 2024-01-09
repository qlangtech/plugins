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





