/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qlangtech.tis.plugin.ds.mysql;

import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import com.qlangtech.tis.trigger.util.JsonUtil;
import com.qlangtech.tis.util.IPluginContext;
import junit.framework.TestCase;
import org.junit.Ignore;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.util.Collections;
import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-12-30 10:56
 **/
public class TestMySQLV5DataSourceFactory extends TestCase {
    public void testGetTableMetadata() {
        IPluginContext pluginContext = IPluginContext.namedContext("test");
        MySQLV5DataSourceFactory dataSourceFactory = new MySQLV5DataSourceFactory();
        dataSourceFactory.useCompression = true;
        dataSourceFactory.password = "123456";
        dataSourceFactory.dbName = "order2";
        dataSourceFactory.encode = "utf8";
        dataSourceFactory.port = 3306;
        dataSourceFactory.userName = "root";
        dataSourceFactory.nodeDesc = "192.168.28.200";


        List<ColumnMetaData> baseColsMeta = dataSourceFactory.getTableMetadata(false, pluginContext, EntityName.parse("base"));
        assertEquals(8, baseColsMeta.size());


        JsonUtil.assertJSONEqual(TestMySQLV5DataSourceFactory.class, "base-cols-meta.json"
                , JsonUtil.toString(Collections.singletonMap("cols", baseColsMeta)), (msg, e, a) -> {
                    assertEquals(msg, e, a);
                });
    }


    @Ignore
    public void testAliyunAdsSource() {
        MySQLV5DataSourceFactory dataSourceFactory = new MySQLV5DataSourceFactory();
        dataSourceFactory.useCompression = false;
        dataSourceFactory.password = "SLK_20221218";
        dataSourceFactory.dbName = "local-life";
        dataSourceFactory.encode = "utf8";
        dataSourceFactory.port = 3306;
        dataSourceFactory.userName = "slk";
        dataSourceFactory.nodeDesc = "am-2ev66ttmhd5ys6k3l167320o.ads.aliyuncs.com";

        dataSourceFactory.visitFirstConnection((connection) -> {
            try {
//                connection.execute(
//                        "CREATE TABLE `cloudcanal_heartbeat_baisui` (\n" +
//                                "  `id` varchar(32) NOT NULL COMMENT '主键ID',\n" +
//                                "  `name` varchar(50) NOT NULL COMMENT '姓名',\n" +
//                                "  `is_valid` int(1) NOT NULL DEFAULT '1' COMMENT '是否有效，1：有效  0 无效',\n" +
//                                "  `create_time` bigint(20) NOT NULL COMMENT '创建时间',\n" +
//                                "  `op_time` bigint(20) DEFAULT NULL COMMENT '操作时间',\n" +
//                                "  `last_ver` int(11) NOT NULL DEFAULT '1' COMMENT '版本号',\n" +
//                                "  `op_user_id` varchar(32) NOT NULL COMMENT '操作人',\n" +
//                                "  `ext` varchar(1000) DEFAULT NULL,\n" +
//                                "  PRIMARY KEY (`id`)\n" +
//                                ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
//                );

//                connection.execute(
//                        "INSERT INTO `cloudcanal_heartbeat_baisui`(`id`, `name`, `is_valid`, `create_time`, `op_time`, `last_ver`, `op_user_id`, `ext`)\n" +
//                                "VALUES ('3', 'cloudcanal_heartbeat', '1', 1690275535280, 1690791060013, '2', '', NULL)"
//                );

                Connection cnn = connection.getConnection();
                try (PreparedStatement prep = cnn.prepareStatement("insert into `cloudcanal_heartbeat_baisui`(`id`, `name`, `is_valid`, `create_time`, `op_time`, `last_ver`, `op_user_id`, `ext`)" +
                        " values (?,?,?,?,?,?,?,?)")) {

                    prep.setString(1, "11");
                    prep.setString(2, "baisui");
                    prep.setInt(3, 1);
                    prep.setLong(4, System.currentTimeMillis());
                    prep.setLong(5, System.currentTimeMillis());
                    prep.setInt(6, 5);
                    prep.setString(7, "22");
                    prep.setString(8, "xxx");

                    prep.execute();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });

    }
}
