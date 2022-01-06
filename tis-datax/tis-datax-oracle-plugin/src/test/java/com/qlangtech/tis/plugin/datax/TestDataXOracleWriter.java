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

package com.qlangtech.tis.plugin.datax;

import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.extension.util.PluginExtraProps;
import com.qlangtech.tis.plugin.common.PluginDesc;
import com.qlangtech.tis.plugin.common.WriterTemplate;
import com.qlangtech.tis.plugin.datax.test.TestSelectedTabs;
import com.qlangtech.tis.plugin.ds.oracle.OracleDataSourceFactory;
import com.qlangtech.tis.plugin.ds.oracle.TestOracleDataSourceFactory;
import junit.framework.TestCase;

import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-08 11:35
 **/
public class TestDataXOracleWriter extends TestCase {
    public void testGetDftTemplate() {
        String dftTemplate = DataXOracleWriter.getDftTemplate();
        assertNotNull("dftTemplate can not be null", dftTemplate);
    }

    public void testPluginExtraPropsLoad() throws Exception {
        Optional<PluginExtraProps> extraProps = PluginExtraProps.load(DataXOracleWriter.class);
        assertTrue(extraProps.isPresent());
    }

    public void testDescGenerate() throws Exception {

        PluginDesc.testDescGenerate(DataXOracleWriter.class, "oracle-datax-writer-descriptor.json");
    }

    public void testTemplateGenerate() throws Exception {
        OracleDataSourceFactory dsFactory = TestOracleDataSourceFactory.createOracleDataSourceFactory();

        DataXOracleWriter writer = new DataXOracleWriter() {
            @Override
            public OracleDataSourceFactory getDataSourceFactory() {
                return dsFactory;
            }

            @Override
            public Class<?> getOwnerClass() {
                return DataXOracleWriter.class;
            }
        };
        writer.template = DataXOracleWriter.getDftTemplate();
        writer.batchSize = 1235;
        writer.postSql = "drop table @table";
        writer.preSql = "drop table @table";
        writer.dbName = "testdb";
        writer.session = "[\n" +
                "            \"alter session set nls_date_format = 'dd.mm.yyyy hh24:mi:ss';\"\n" +
                "            \"alter session set NLS_LANG = 'AMERICAN';\"\n" +
                "]";

        Optional<IDataxProcessor.TableMap> tableMap = TestSelectedTabs.createTableMapper();


        WriterTemplate.valiateCfgGenerate("oracle-datax-writer-assert.json", writer, tableMap.get());

        writer.session = null;
        writer.preSql = null;
        writer.postSql = null;
        writer.batchSize = null;

        WriterTemplate.valiateCfgGenerate("oracle-datax-writer-assert-without-option.json", writer, tableMap.get());
    }
}
