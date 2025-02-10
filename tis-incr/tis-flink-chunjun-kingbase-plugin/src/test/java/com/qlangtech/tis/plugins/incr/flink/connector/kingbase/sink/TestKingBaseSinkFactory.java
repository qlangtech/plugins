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

package com.qlangtech.tis.plugins.incr.flink.connector.kingbase.sink;

import com.qlangtech.plugins.incr.flink.chunjun.doris.sink.TestFlinkSinkExecutor;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsWriter;
import com.qlangtech.tis.plugin.datax.kingbase.DataXKingBaseWriter;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.ds.kingbase.KingBaseDataSourceFactory;
import com.qlangtech.tis.plugin.ds.oracle.KingBaseDSFactoryContainer;
import com.qlangtech.tis.plugins.incr.flink.connector.ChunjunSinkFactory;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-01-26 20:34
 **/
public class TestKingBaseSinkFactory extends TestFlinkSinkExecutor {


    @Test
    public void testKingBaseWrite() throws Exception {
        super.testSinkSync();
    }

    static KingBaseDataSourceFactory kingBaseDSFactory;

    @Override
    protected DataxReader createDataxReader() {
        DataxReader dataxReader = super.createDataxReader();
        DataxReader.dataxReaderThreadLocal.set(dataxReader);
        return dataxReader;
    }

    @BeforeClass
    public static void initialize() throws Exception {
        //  MySqlSourceTestBase.startContainers();

        kingBaseDSFactory = (KingBaseDataSourceFactory) KingBaseDSFactoryContainer.initialize((conn) -> {
        });
        // tableName
        kingBaseDSFactory.visitAllConnection((conn) -> {
            try {
                conn.execute("drop table " + kingBaseDSFactory.getEscapedEntity(tableName));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        });
        // kingBaseDSFactory = (KingBaseDataSourceFactory) MySqlContainer.MYSQL5_CONTAINER.createMySqlDataSourceFactory(new TargetResName(dataXName));
    }

    @Override
    protected CMeta createUpdateTime() {
        CMeta updateTime = super.createUpdateTime();
        updateTime.setPk(false);
        return updateTime;
    }

    @Override
    protected BasicDataSourceFactory getDsFactory() {
        return kingBaseDSFactory;
    }

    @Override
    protected ChunjunSinkFactory getSinkFactory() {
        KingBaseSinkFactory sinkFactory = new KingBaseSinkFactory();
        return sinkFactory;
    }

    @Override
    protected BasicDataXRdbmsWriter createDataXWriter() {
        DataXKingBaseWriter dataXWriter = new DataXKingBaseWriter() {
            @Override
            public KingBaseDataSourceFactory getDataSourceFactory() {
                return kingBaseDSFactory;
            }
        };

        return dataXWriter;
    }
}
