CREATE TABLE application
(
    `user_id`     BIGINT,
    `id3`         BIGINT,
    `col6`        VARCHAR(256),
    `user_name`   VARCHAR(256),
    `col4`        VARCHAR(256),
    `col5`        VARCHAR(256),
    `col5`        VARCHAR(256)
)
 ENGINE=olap
UNIQUE KEY(`user_id`,`id3`,`col6`)
DISTRIBUTED BY HASH(`user_id`,`id3`,`col6`)
BUCKETS 10
PROPERTIES("replication_num" = "1")
