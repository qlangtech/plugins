-- Licensed to the Apache Software Foundation (ASF) under one
-- or more contributor license agreements.  See the NOTICE file
-- distributed with this work for additional information
-- regarding copyright ownership.  The ASF licenses this file
-- to you under the Apache License, Version 2.0 (the
-- "License"); you may not use this file except in compliance
-- with the License.  You may obtain a copy of the License at
--   http://www.apache.org/licenses/LICENSE-2.0
-- Unless required by applicable law or agreed to in writing,
-- software distributed under the License is distributed on an
-- "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
-- KIND, either express or implied.  See the License for the
-- specific language governing permissions and limitations
-- under the License.

-- In production you would almost certainly limit the replication user must be on the follower (slave) machine,
-- to prevent other clients accessing the log from other machines. For example, 'replicator'@'follower.acme.com'.
-- However, in this database we'll grant 2 users different privileges:
--
-- 1) 'flinkuser' - all privileges required by the snapshot reader AND binlog reader (used for testing)
-- 2) 'mysqluser' - all privileges
--
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT, LOCK TABLES  ON *.* TO 'flinkuser'@'%';
CREATE USER 'mysqluser' IDENTIFIED BY 'mysqlpw';
GRANT ALL PRIVILEGES ON *.* TO 'mysqluser'@'%';

-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  emptydb
-- ----------------------------------------------------------------------------------------------------------------
-- CREATE DATABASE flink-test;
use `flink-test`;

CREATE TABLE `base` (
  `base_id` int(11) NOT NULL,
  `start_time` datetime DEFAULT NULL,
  `update_date` date DEFAULT NULL,
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `price` decimal(5,2) DEFAULT NULL,
  `json_content` json DEFAULT NULL,
  `col_blob` blob,
  `col_text` text,
  PRIMARY KEY (`base_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `base_01` (
  `base_id` int(11) NOT NULL,
  `start_time` datetime DEFAULT NULL,
  `update_date` date DEFAULT NULL,
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `price` decimal(5,2) DEFAULT NULL,
  `json_content` json DEFAULT NULL,
  `col_blob` blob,
  `col_text` text,
  PRIMARY KEY (`base_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

CREATE TABLE `base_02` (
  `base_id` int(11) NOT NULL,
  `start_time` datetime DEFAULT NULL,
  `update_date` date DEFAULT NULL,
  `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
  `price` decimal(5,2) DEFAULT NULL,
  `json_content` json DEFAULT NULL,
  `col_blob` blob,
  `col_text` text,
  PRIMARY KEY (`base_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;

insert into `base` (`base_id`,`start_time`,`update_date`,`update_time`,`price`,`json_content`,`col_blob`,`col_text`)
values (1,now(),now(),now(),1.1,'{}','123','123');

insert into `base` (`base_id`,`start_time`,`update_date`,`update_time`,`price`,`json_content`,`col_blob`,`col_text`)
values (2,now(),now(),now(),1.1,'{}','321','321');

insert into `base_01` (`base_id`,`start_time`,`update_date`,`update_time`,`price`,`json_content`,`col_blob`,`col_text`)
values (1,now(),now(),now(),1.1,'{}','123','123');

insert into `base_02` (`base_id`,`start_time`,`update_date`,`update_time`,`price`,`json_content`,`col_blob`,`col_text`)
values (2,now(),now(),now(),1.1,'{}','321','321');



CREATE TABLE `instancedetail` (
  `instance_id` varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `order_id` varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `batch_msg` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT '' COMMENT '批次信息：时:分 点菜人',
  `type` smallint(3) NOT NULL DEFAULT '0' COMMENT '商品类型(0:菜 1: 打包盒)',
  `ext` text COLLATE utf8mb4_unicode_ci NOT NULL COMMENT '扩展字段，不适合做查询字段',
  `waitinginstance_id` varchar(32) COLLATE utf8mb4_unicode_ci DEFAULT '',
  `kind` smallint(6) NOT NULL DEFAULT '1' COMMENT '1:普通菜;2:套菜;3:自定义菜;4:自定义套菜',
  `parent_id` varchar(32) COLLATE utf8mb4_unicode_ci DEFAULT '',
  `pricemode` smallint(6) NOT NULL DEFAULT '1' COMMENT '1:固定价;2:浮动价',
  `name` varchar(50) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `makename` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT '',
  `taste` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT '',
  `spec_detail_name` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT '',
  `num` decimal(10,2) NOT NULL DEFAULT '0.00',
  `account_num` decimal(12,4) NOT NULL DEFAULT '0.0000',
  `unit` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT '',
  `account_unit` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT '',
  `price` decimal(18,2) NOT NULL DEFAULT '0.00',
  `member_price` decimal(18,2) NOT NULL DEFAULT '0.00',
  `fee` decimal(18,2) NOT NULL DEFAULT '0.00',
  `ratio` decimal(10,2) NOT NULL DEFAULT '0.00',
  `ratio_fee` decimal(18,2) NOT NULL DEFAULT '0.00',
  `ratio_cause` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT '',
  `status` smallint(6) NOT NULL DEFAULT '1' COMMENT '状态：1/未确认 2/正常 3/退菜标志',
  `kindmenu_id` varchar(32) COLLATE utf8mb4_unicode_ci DEFAULT '',
  `kindmenu_name` varchar(50) COLLATE utf8mb4_unicode_ci DEFAULT '',
  `menu_id` varchar(32) COLLATE utf8mb4_unicode_ci DEFAULT '',
  `memo` varchar(255) COLLATE utf8mb4_unicode_ci DEFAULT '',
  `is_ratio` smallint(6) DEFAULT '0',
  `entity_id` varchar(32) COLLATE utf8mb4_unicode_ci NOT NULL DEFAULT '',
  `is_valid` smallint(6) NOT NULL DEFAULT '1',
  `create_time` bigint(20) DEFAULT '0',
  `op_time` bigint(20) DEFAULT '0',
  `last_ver` bigint(20) NOT NULL DEFAULT '0',
  `load_time` int(11) NOT NULL DEFAULT '0' COMMENT '记录执行的服务器时间',
  `modify_time` int(11) NOT NULL DEFAULT '0' COMMENT '记录修改的服务器时间',
  `draw_status` tinyint(4) NOT NULL DEFAULT '0',
  `bookmenu_id` varchar(32) COLLATE utf8mb4_unicode_ci DEFAULT '',
  `make_id` varchar(32) COLLATE utf8mb4_unicode_ci DEFAULT '',
  `make_price` decimal(18,2) NOT NULL DEFAULT '0.00',
  `prodplan_id` varchar(32) COLLATE utf8mb4_unicode_ci DEFAULT '',
  `is_wait` tinyint(4) NOT NULL DEFAULT '0',
  `specdetail_id` varchar(32) COLLATE utf8mb4_unicode_ci DEFAULT '',
  `specdetail_price` decimal(18,2) NOT NULL DEFAULT '0.00',
  `makeprice_mode` tinyint(4) NOT NULL DEFAULT '0',
  `original_price` varchar(32) COLLATE utf8mb4_unicode_ci DEFAULT '',
  `is_buynumber_changed` tinyint(4) NOT NULL DEFAULT '0',
  `ratio_operator_id` varchar(32) COLLATE utf8mb4_unicode_ci DEFAULT '',
  `child_id` varchar(32) COLLATE utf8mb4_unicode_ci DEFAULT '',
  `kind_bookmenu_id` varchar(32) COLLATE utf8mb4_unicode_ci DEFAULT '',
  `specprice_mode` tinyint(4) NOT NULL DEFAULT '0',
  `worker_id` varchar(32) COLLATE utf8mb4_unicode_ci DEFAULT '',
  `is_backauth` tinyint(4) NOT NULL DEFAULT '1',
  `service_fee_mode` tinyint(4) NOT NULL DEFAULT '0',
  `service_fee` varchar(32) COLLATE utf8mb4_unicode_ci DEFAULT '',
  `orign_id` varchar(32) COLLATE utf8mb4_unicode_ci DEFAULT '',
  `addition_price` decimal(18,2) NOT NULL DEFAULT '0.00',
  `has_addition` tinyint(4) NOT NULL DEFAULT '0',
  `seat_id` varchar(32) COLLATE utf8mb4_unicode_ci DEFAULT '',
  PRIMARY KEY (`instance_id`),
  KEY `idx_load_time` (`load_time`),
  KEY `idx_modify_time` (`modify_time`),
  KEY `idx_oid_eid` (`order_id`,`entity_id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8;


CREATE TABLE `stu` (
  `id` int(10) unsigned NOT NULL AUTO_INCREMENT COMMENT '',
  `name` varchar(20) NOT NULL COMMENT '',
  `school` varchar(20) NOT NULL COMMENT '',
  `nickname` varchar(20) NOT NULL COMMENT '',
  `age` int(11) NOT NULL COMMENT '',
  `class_num` int(11) NOT NULL COMMENT '班级人数',
  `score` decimal(4,2) NOT NULL COMMENT '成绩',
  `phone` bigint(20) NOT NULL COMMENT '电话号码',
  `email` varchar(64) DEFAULT NULL COMMENT '',
  `ip` varchar(32) DEFAULT NULL COMMENT '',
  `address` text COMMENT '',
  PRIMARY KEY (`id`)
) ENGINE=InnoDB AUTO_INCREMENT=1100002 DEFAULT CHARSET=utf8;


CREATE TABLE full_types (
    id INT AUTO_INCREMENT NOT NULL,
    tiny_c TINYINT,
    tiny_un_c TINYINT UNSIGNED,
    small_c SMALLINT,
    small_un_c SMALLINT UNSIGNED,
    medium_c MEDIUMINT,
    medium_un_c MEDIUMINT UNSIGNED,
    int_c INTEGER ,
    int_un_c INTEGER UNSIGNED,
    int11_c INT(11) ,
    big_c BIGINT,
    big_un_c BIGINT UNSIGNED,
    varchar_c VARCHAR(255),
    char_c CHAR(3),
    real_c REAL,
    float_c FLOAT,
    double_c DOUBLE,
    decimal_c DECIMAL(8, 4),
    numeric_c NUMERIC(6, 0),
    big_decimal_c DECIMAL(65, 1),
    bit1_c BIT,
    tiny1_c TINYINT(1),
    boolean_c BOOLEAN,
    date_c DATE,
    time_c TIME(0),
    datetime3_c DATETIME(3),
    datetime6_c DATETIME(6),
    timestamp_c TIMESTAMP,
    file_uuid BINARY(16),
    bit_c BIT(64),
    text_c TEXT,
    tiny_blob_c TINYBLOB,
    blob_c BLOB,
    medium_blob_c MEDIUMBLOB,
    long_blob_c LONGBLOB,
    year_c YEAR,
    enum_c enum('red', 'white') default 'red',
    set_c SET('a', 'b'),
    json_c JSON,
--    point_c POINT,
--    geometry_c GEOMETRY,
--     linestring_c LINESTRING,
--     polygon_c POLYGON,
--     multipoint_c MULTIPOINT,
--     multiline_c MULTILINESTRING,
--     multipolygon_c MULTIPOLYGON,
--     geometrycollection_c GEOMETRYCOLLECTION,
    PRIMARY KEY (id)
) DEFAULT CHARSET=utf8;

INSERT INTO full_types VALUES (
    DEFAULT, 127, 255, 32767, 65535, 8388607, 16777215, 2147483647, 4294967295, 2147483647, 9223372036854775807,
    18446744073709551615,
    'Hello World', 'abc', 123.102, 123.102, 404.4443, 123.4567, 345.6, 34567892.1, 0, 1, true,
    '2020-07-17', '18:00:22', '2020-07-17 18:00:22.123', '2020-07-17 18:00:22.123456', '2020-07-17 18:00:22',
    unhex(replace('651aed08-390f-4893-b2f1-36923e7b7400','-','')),
     b'0000010000000100000001000000010000000100000001000000010000000100',
    'text',UNHEX(HEX(16)),UNHEX(HEX(16)),UNHEX(HEX(16)),UNHEX(HEX(16)), 2021,
    'red', 'a,b,a', '{"key1": "value1"}'
--     ,ST_GeomFromText('POINT(1 1)'),
--     ST_GeomFromText('POLYGON((1 1, 2 1, 2 2,  1 2, 1 1))'),
--     ST_GeomFromText('LINESTRING(3 0, 3 3, 3 5)'),
--     ST_GeomFromText('POLYGON((1 1, 2 1, 2 2,  1 2, 1 1))'),
--     ST_GeomFromText('MULTIPOINT((1 1),(2 2))'),
--     ST_GeomFromText('MultiLineString((1 1,2 2,3 3),(4 4,5 5))'),
--     ST_GeomFromText('MULTIPOLYGON(((0 0, 10 0, 10 10, 0 10, 0 0)), ((5 5, 7 5, 7 7, 5 7, 5 5)))'),
--     ST_GeomFromText('GEOMETRYCOLLECTION(POINT(10 10), POINT(30 30), LINESTRING(15 15, 20 20))')
);

