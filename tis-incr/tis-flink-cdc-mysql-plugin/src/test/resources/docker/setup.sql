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

