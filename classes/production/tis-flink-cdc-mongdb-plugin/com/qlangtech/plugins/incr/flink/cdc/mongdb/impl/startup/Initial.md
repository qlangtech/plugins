## copyExistingMaxThreads

The number of threads to use when performing the data copy. Defaults to the number of
 * processors. Default: defaults to the number of processors

## copyExistingQueueSize
The max size of the queue to use when copying data. 
* Default: `10240`

## pollMaxBatchSize

Maximum number of change stream documents to include in a single batch when polling for new data. This setting can be used to limit the amount of data buffered internally in the connector.
* Default: `1024`

## pollAwaitTimeMillis

The amount of time to wait before checking for new results on the change stream.
* Default: `1000`

## heartbeatIntervalMillis

The length of time in milliseconds between sending heartbeat messages. Heartbeat messages contain the post batch resume token and are sent when no source records have been published in the specified interval. 
This improves the resumability of the connector for low volume namespaces. 

Use `0` to disable.
