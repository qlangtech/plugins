-- Licensed to the Apache Software Foundation (ASF) under one or more
-- contributor license agreements.  See the NOTICE file distributed with
-- this work for additional information regarding copyright ownership.
-- The ASF licenses this file to You under the Apache License, Version 2.0
-- (the "License"); you may not use this file except in compliance with
-- the License.  You may obtain a copy of the License at
-- 
--      http://www.apache.org/licenses/LICENSE-2.0
-- 
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

-- ----------------------------------------------------------------------------------------------------------------
-- DATABASE:  customer
-- ----------------------------------------------------------------------------------------------------------------

-- Create and populate our users using a single insert with many rows
--CREATE TABLE DEBEZIUM.CUSTOMERS (
--  ID INT NOT NULL,
--  NAME VARCHAR2(255) NOT NULL,
--  ADDRESS VARCHAR2(1024),
--  PHONE_NUMBER VARCHAR2(512),
--  PRIMARY KEY(ID)
--);
ALTER SESSION SET TIME_ZONE='Asia/Shanghai';

CREATE TABLE DEBEZIUM."base" (
  "base_id"        NUMBER          NOT NULL,
  "start_time"     TIMESTAMP       NULL,
  "update_date"    DATE            NULL,
  "update_time"    TIMESTAMP       DEFAULT CURRENT_TIMESTAMP NOT NULL,
  "price"          NUMBER(5, 2)    NULL,
  "json_content"   CLOB            NULL,
  "col_blob"       BLOB            NULL,
  "col_text"       CLOB            NULL,
  CONSTRAINT PK_base PRIMARY KEY ("base_id")
)
LOB ("json_content", "col_blob", "col_text") STORE AS (TABLESPACE USERS);

-- 如果需要指定表空间，可以使用如下语法：
-- CREATE TABLE base (...) TABLESPACE your_tablespace_name;
-- 对于 LOB 列（如 CLOB 和 BLOB），可以单独指定它们的存储参数：
-- LOB (json_content, col_blob, col_text) STORE AS (TABLESPACE lob_tablespace_name);

ALTER TABLE DEBEZIUM."base" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;


CREATE TABLE DEBEZIUM."base_without_lob" (
  "base_id"        NUMBER          NOT NULL,
  "start_time"     TIMESTAMP       NULL,
  "update_date"    DATE            NULL,
  "update_time"    TIMESTAMP       DEFAULT CURRENT_TIMESTAMP NOT NULL,
  "price"          NUMBER(5, 2)    NULL,
  CONSTRAINT PK_base_without_lob PRIMARY KEY ("base_id")
);

ALTER TABLE DEBEZIUM."base_without_lob" ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;

--INSERT INTO DEBEZIUM."base" (
--  "base_id",
--  "start_time",
--  "update_date",
--  "price",
--  "json_content",
--  "col_blob",
--  "col_text"
--) VALUES (
--  1, -- 假设 base_id 是 1
--  TO_TIMESTAMP('2024-12-01 12:00:00', 'YYYY-MM-DD HH24:MI:SS'), -- 示例 start_time
--  TO_DATE('2024-12-01', 'YYYY-MM-DD'), -- 示例 update_date
--  99.99, -- 示例 price
--  TO_CLOB('{"key":"value"}'), -- 示例 json_content
--  UTL_RAW.CAST_TO_RAW('Example Blob Data'), -- 示例 col_blob
--  TO_CLOB('Example Text Data') -- 示例 col_text
--);

--INSERT INTO DEBEZIUM.CUSTOMERS VALUES (101,'user_1','Shanghai','123567891234');
--INSERT INTO DEBEZIUM.CUSTOMERS VALUES (102,'user_2','Shanghai','123567891234');
--INSERT INTO DEBEZIUM.CUSTOMERS VALUES (103,'user_3','Shanghai','123567891234');
--INSERT INTO DEBEZIUM.CUSTOMERS VALUES (109,'user_4','Shanghai','123567891234');
--INSERT INTO DEBEZIUM.CUSTOMERS VALUES (110,'user_5','Shanghai','123567891234');
--INSERT INTO DEBEZIUM.CUSTOMERS VALUES (111,'user_6','Shanghai','123567891234');
--INSERT INTO DEBEZIUM.CUSTOMERS VALUES (118,'user_7','Shanghai','123567891234');
--INSERT INTO DEBEZIUM.CUSTOMERS VALUES (121,'user_8','Shanghai','123567891234');
--INSERT INTO DEBEZIUM.CUSTOMERS VALUES (123,'user_9','Shanghai','123567891234');
--INSERT INTO DEBEZIUM.CUSTOMERS VALUES (1009,'user_10','Shanghai','123567891234');
--INSERT INTO DEBEZIUM.CUSTOMERS VALUES (1010,'user_11','Shanghai','123567891234');
--INSERT INTO DEBEZIUM.CUSTOMERS VALUES (1011,'user_12','Shanghai','123567891234');
--INSERT INTO DEBEZIUM.CUSTOMERS VALUES (1012,'user_13','Shanghai','123567891234');
--INSERT INTO DEBEZIUM.CUSTOMERS VALUES (1013,'user_14','Shanghai','123567891234');
--INSERT INTO DEBEZIUM.CUSTOMERS VALUES (1014,'user_15','Shanghai','123567891234');
--INSERT INTO DEBEZIUM.CUSTOMERS VALUES (1015,'user_16','Shanghai','123567891234');
--INSERT INTO DEBEZIUM.CUSTOMERS VALUES (1016,'user_17','Shanghai','123567891234');
--INSERT INTO DEBEZIUM.CUSTOMERS VALUES (1017,'user_18','Shanghai','123567891234');
--INSERT INTO DEBEZIUM.CUSTOMERS VALUES (1018,'user_19','Shanghai','123567891234');
--INSERT INTO DEBEZIUM.CUSTOMERS VALUES (1019,'user_20','Shanghai','123567891234');
--INSERT INTO DEBEZIUM.CUSTOMERS VALUES (2000,'user_21','Shanghai','123567891234');
--
---- table has same name prefix with 'customers.*'
--CREATE TABLE DEBEZIUM.CUSTOMERS_1 (
--   ID INT NOT NULL,
--   NAME VARCHAR2(255) NOT NULL,
--   ADDRESS VARCHAR2(1024),
--   PHONE_NUMBER VARCHAR2(512),
--   PRIMARY KEY(ID)
--);
--
--INSERT INTO DEBEZIUM.CUSTOMERS_1 VALUES (101,'user_1','Shanghai','123567891234');
--INSERT INTO DEBEZIUM.CUSTOMERS_1 VALUES (102,'user_2','Shanghai','123567891234');
--INSERT INTO DEBEZIUM.CUSTOMERS_1 VALUES (103,'user_3','Shanghai','123567891234');
--INSERT INTO DEBEZIUM.CUSTOMERS_1 VALUES (109,'user_4','Shanghai','123567891234');
--INSERT INTO DEBEZIUM.CUSTOMERS_1 VALUES (110,'user_5','Shanghai','123567891234');
--INSERT INTO DEBEZIUM.CUSTOMERS_1 VALUES (111,'user_6','Shanghai','123567891234');
--INSERT INTO DEBEZIUM.CUSTOMERS_1 VALUES (118,'user_7','Shanghai','123567891234');
--INSERT INTO DEBEZIUM.CUSTOMERS_1 VALUES (121,'user_8','Shanghai','123567891234');
--INSERT INTO DEBEZIUM.CUSTOMERS_1 VALUES (123,'user_9','Shanghai','123567891234');
--INSERT INTO DEBEZIUM.CUSTOMERS_1 VALUES (1009,'user_10','Shanghai','123567891234');
--INSERT INTO DEBEZIUM.CUSTOMERS_1 VALUES (1010,'user_11','Shanghai','123567891234');
--INSERT INTO DEBEZIUM.CUSTOMERS_1 VALUES (1011,'user_12','Shanghai','123567891234');
--INSERT INTO DEBEZIUM.CUSTOMERS_1 VALUES (1012,'user_13','Shanghai','123567891234');
--INSERT INTO DEBEZIUM.CUSTOMERS_1 VALUES (1013,'user_14','Shanghai','123567891234');
--INSERT INTO DEBEZIUM.CUSTOMERS_1 VALUES (1014,'user_15','Shanghai','123567891234');
--INSERT INTO DEBEZIUM.CUSTOMERS_1 VALUES (1015,'user_16','Shanghai','123567891234');
--INSERT INTO DEBEZIUM.CUSTOMERS_1 VALUES (1016,'user_17','Shanghai','123567891234');
--INSERT INTO DEBEZIUM.CUSTOMERS_1 VALUES (1017,'user_18','Shanghai','123567891234');
--INSERT INTO DEBEZIUM.CUSTOMERS_1 VALUES (1018,'user_19','Shanghai','123567891234');
--INSERT INTO DEBEZIUM.CUSTOMERS_1 VALUES (1019,'user_20','Shanghai','123567891234');
--INSERT INTO DEBEZIUM.CUSTOMERS_1 VALUES (2000,'user_21','Shanghai','123567891234');
--
---- table has combined primary key and one of the primary key is evenly
--CREATE TABLE EVENLY_SHOPPING_CART (
--  PRODUCT_NO INT NOT NULL,
--  PRODUCT_KIND VARCHAR(255),
--  USER_ID VARCHAR(255) NOT NULL,
--  DESCRIPTION VARCHAR(255) NOT NULL,
--  PRIMARY KEY(PRODUCT_KIND, PRODUCT_NO, USER_ID)
--);
--
--ALTER TABLE DEBEZIUM.EVENLY_SHOPPING_CART ADD SUPPLEMENTAL LOG DATA (ALL) COLUMNS;
--
--INSERT INTO EVENLY_SHOPPING_CART VALUES (101, 'KIND_001', 'user_1', 'my shopping cart');
--INSERT INTO EVENLY_SHOPPING_CART VALUES (102, 'KIND_002', 'user_1', 'my shopping cart');
--INSERT INTO EVENLY_SHOPPING_CART VALUES (103, 'KIND_007', 'user_1', 'my shopping cart');
--INSERT INTO EVENLY_SHOPPING_CART VALUES (104, 'KIND_008', 'user_1', 'my shopping cart');
--INSERT INTO EVENLY_SHOPPING_CART VALUES (105, 'KIND_100', 'user_2', 'my shopping list');
--INSERT INTO EVENLY_SHOPPING_CART VALUES (105, 'KIND_999', 'user_3', 'my shopping list');
--INSERT INTO EVENLY_SHOPPING_CART VALUES (107, 'KIND_010', 'user_4', 'my shopping list');
--INSERT INTO EVENLY_SHOPPING_CART VALUES (108, 'KIND_009', 'user_4', 'my shopping list');
--INSERT INTO EVENLY_SHOPPING_CART VALUES (109, 'KIND_002', 'user_5', 'leo list');
--INSERT INTO EVENLY_SHOPPING_CART VALUES (111, 'KIND_007', 'user_5', 'leo list');
--INSERT INTO EVENLY_SHOPPING_CART VALUES (111, 'KIND_008', 'user_5', 'leo list');
--INSERT INTO EVENLY_SHOPPING_CART VALUES (112, 'KIND_009', 'user_6', 'my shopping cart');
