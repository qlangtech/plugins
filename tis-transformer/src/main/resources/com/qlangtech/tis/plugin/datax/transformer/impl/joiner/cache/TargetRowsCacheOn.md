## maxSize
Specifies the maximum number of entries the cache may contain. Note that the cache <b>may evict an entry before this limit is exceeded or temporarily exceed the threshold while evicting</b>.
As the cache size grows close to the maximum, the cache evicts entries that are less likely to be used again. For example, the cache may evict an entry because it hasn't been used recently or very often.

## expireAfterWrite

缓存项创建后的过期时间
Specifies that each entry should be automatically removed from the cache once a fixed duration has elapsed after the entry's creation, or the most recent replacement of its value.

## expireAfterAccess

缓存项最后访问后的过期时间
Specifies that each entry should be automatically removed from the cache once a fixed duration has elapsed after the entry's creation, the most recent replacement of its value, or its last access. 
Access time is reset by all cache read and write operations, but not by operations on the collection-views .
