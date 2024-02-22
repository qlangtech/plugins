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
-- https://www.postgresql.org/docs/current/sql-createtable.html
-- Create the schema that we'll use to populate data and watch the effect in the binlog
ALTER DATABASE postgres SET timezone TO 'Asia/Shanghai';
ALTER SYSTEM SET shared_preload_libraries = 'pgoutput';
-- 修改一些参数配置，修改后需重启数据库，生产环境根据实际情况再进行对应调整
-- 修改wal级别为logical
ALTER SYSTEM SET wal_level = logical;
-- 修改能支持的复制槽的最大数量（默认值：10）
ALTER SYSTEM SET max_replication_slots = 200;
-- 指定来自后备服务器或流式基础备份客户端的并发连接的最大数量（即同时运行 WAL 发送进程的最大数）（默认值：10）
ALTER SYSTEM SET max_wal_senders = 50;
-- 修改最大连接数（默认值：100）
ALTER SYSTEM SET max_connections = 1000;
DROP SCHEMA IF EXISTS tis CASCADE;
CREATE SCHEMA tis;
SET search_path TO tis;

-- Create and populate our products using a single insert with many rows
-- https://www.postgresql.org/docs/current/datatype.html
CREATE TABLE tis.base (
   base_id integer NOT NULL PRIMARY KEY,
   testid numeric DEFAULT NULL,
   start_time timestamp DEFAULT NULL,
   update_date date DEFAULT NULL,
   update_time timestamp NOT NULL ,
   price decimal(5,2) DEFAULT NULL,
   json_content json DEFAULT NULL,
   col_blob bytea,
   col_text text
) ;



--ALTER SEQUENCE products_id_seq RESTART WITH 101;
ALTER TABLE base REPLICA IDENTITY FULL;

-- INSERT INTO products
-- VALUES (default,'scooter','Small 2-wheel scooter',3.14),
--        (default,'car battery','12V car battery',8.1),
--        (default,'12-pack drill bits','12-pack of drill bits with sizes ranging from #40 to #3',0.8),
--        (default,'hammer','12oz carpenter''s hammer',0.75),
--        (default,'hammer','14oz carpenter''s hammer',0.875),
--        (default,'hammer','16oz carpenter''s hammer',1.0),
--        (default,'rocks','box of assorted rocks',5.3),
--        (default,'jacket','water resistent black wind breaker',0.1),
--        (default,'spare tire','24 inch spare tire',22.2);
