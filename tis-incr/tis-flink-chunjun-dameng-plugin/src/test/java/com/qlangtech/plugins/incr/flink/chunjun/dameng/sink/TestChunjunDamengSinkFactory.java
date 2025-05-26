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

package com.qlangtech.plugins.incr.flink.chunjun.dameng.sink;

import com.qlangtech.plugins.incr.flink.chunjun.doris.sink.TestChunjunFlinkSinkExecutor;
import com.qlangtech.plugins.incr.flink.chunjun.doris.sink.TestFlinkSinkExecutor;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsWriter;
import com.qlangtech.tis.plugin.datax.dameng.ds.DaMengDataSourceFactory;
import com.qlangtech.tis.plugin.datax.dameng.writer.DataXDaMengWriter;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.oracle.DamengDSFactoryContainer;
import com.qlangtech.tis.plugin.ds.oracle.TISDamengContainer;
import com.qlangtech.tis.plugins.incr.flink.connector.ChunjunSinkFactory;
import com.qlangtech.tis.plugins.incr.flink.connector.UpdateMode;
import com.qlangtech.tis.plugins.incr.flink.connector.impl.UpsertType;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-08-24 13:39
 **/
public class TestChunjunDamengSinkFactory extends TestChunjunFlinkSinkExecutor {

//    // docker run -d -p 1521:1521 -e ORACLE_PASSWORD=test -e ORACLE_DATABASE=tis gvenzl/oracle-xe:18.4.0-slim
//    public static final DockerImageName ORACLE_DOCKER_IMAGE_NAME = DockerImageName.parse(
//            "gvenzl/oracle-xe:18.4.0-slim"
//            // "registry.cn-hangzhou.aliyuncs.com/tis/oracle-xe:18.4.0-slim"
//    );

    public static BasicDataSourceFactory oracleDS;
    private static TISDamengContainer damengContainer;


    @BeforeClass
    public static void initialize() {

        DamengDSFactoryContainer.initialize(true);
        damengContainer = DamengDSFactoryContainer.damengContainer;
        oracleDS = DamengDSFactoryContainer.damengDS;
    }

    @AfterClass
    public static void stop() {

        damengContainer.close();
    }

    @Override
    protected UpdateMode createIncrMode() {
        //  InsertType insertType = new InsertType();
        UpsertType updateMode = new UpsertType();
        // UpdateType updateMode = new UpdateType();
        //  updateMode.updateKey = Lists.newArrayList(colId, updateTime);
        return updateMode;
    }

//    @Override
//    protected ArrayList<String> getUniqueKey(List<CMeta> metaCols) {
//        //  return Lists.newArrayList(colId, updateTime);
//        return super.getUniqueKey(metaCols);
//    }

    @Override
    protected BasicDataSourceFactory getDsFactory() {
        return oracleDS;
    }

    @Test
    @Override
    public void testSinkSync() throws Exception {

        super.testSinkSync();
    }

    @Override
    protected ChunjunSinkFactory createSinkFactory() {
        return new ChunjunDamengSinkFactory();
    }

    @Override
    protected BasicDataXRdbmsWriter createDataXWriter() {
        DataXDaMengWriter writer = new DataXDaMengWriter() {
            @Override
            public DaMengDataSourceFactory getDataSourceFactory() {
                return (DaMengDataSourceFactory) oracleDS;
            }
        };
        return writer;
    }

}
