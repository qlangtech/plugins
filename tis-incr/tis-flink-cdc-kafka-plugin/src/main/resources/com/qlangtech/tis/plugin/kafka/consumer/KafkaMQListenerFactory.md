## startOffset

Kafka消费起始位置，有以下策略可供选择

* `Committed Offset`: Start from committed offset of the consuming group, without reset strategy. An exception will be
  thrown at runtime if there is no committed offsets.
* `Earliest Offset`: Start from earliest offset
* `Earliest When None Committed Offset`:  Start from committed offset, also use EARLIEST as reset strategy if committed
  offset doesn't exist
* `Latest Offset`: (default) Start from latest offset
* `Timestamp Offset`: Start from the first record whose timestamp is greater than or equals a timestamp (milliseconds)