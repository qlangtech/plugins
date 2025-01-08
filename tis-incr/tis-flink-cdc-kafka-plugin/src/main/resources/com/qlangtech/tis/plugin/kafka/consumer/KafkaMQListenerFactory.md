## startOffset

Kafka消费起始位置，有以下策略可供选择

* `Committed Offset`: Start from committed offset of the consuming group, without reset strategy. An exception will be
  thrown at runtime if there is no committed offsets.
* `Earliest Offset`: Start from earliest offset
* `Earliest When None Committed Offset`:  Start from committed offset, also use EARLIEST as reset strategy if committed
  offset doesn't exist
* `Latest Offset`: (default) Start from latest offset
* `Timestamp Offset`: Start from the first record whose timestamp is greater than or equals a timestamp (milliseconds)

## independentBinLogMonitor

执行Flink任务过程中，监听分配独立的Slot计算资源不会与下游计算算子混合在一起。

如开启，带来的好处是运算时资源各自独立不会相互相互影响，弊端是，上游算子与下游算子独立在两个Solt中需要额外的网络传输开销