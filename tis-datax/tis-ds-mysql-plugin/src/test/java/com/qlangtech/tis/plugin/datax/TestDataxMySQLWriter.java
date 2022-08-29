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

import com.alibaba.citrus.turbine.Context;
import com.google.common.collect.Lists;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.datax.*;
import com.qlangtech.tis.datax.impl.DataXCfgGenerator;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.PluginFormProperties;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.extension.impl.RootFormProperties;
import com.qlangtech.tis.extension.util.PluginExtraProps;
import com.qlangtech.tis.plugin.BasicTest;
import com.qlangtech.tis.plugin.datax.test.TestSelectedTabs;
import com.qlangtech.tis.plugin.ds.*;
import com.qlangtech.tis.plugin.ds.mysql.MySQLDataSourceFactory;
import com.qlangtech.tis.trigger.util.JsonUtil;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.lang.StringUtils;
import org.easymock.EasyMock;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * @author: baisui 百岁
 * @create: 2021-04-15 16:10
 **/
public class TestDataxMySQLWriter extends BasicTest {
    public static String mysqlJdbcUrl = "jdbc:mysql://192.168.28.200:3306/baisuitestWriterdb?useUnicode=yes&characterEncoding=utf8";
    public static String dbWriterName = "baisuitestWriterdb";

    public static String dataXName = "testDataXName";

    public void testFieldCount() throws Exception {
        DataxMySQLWriter mySQLWriter = new DataxMySQLWriter();
        Descriptor<DataxWriter> descriptor = mySQLWriter.getDescriptor();
        PluginFormProperties pluginFormPropertyTypes = descriptor.getPluginFormPropertyTypes();

        assertTrue(pluginFormPropertyTypes instanceof RootFormProperties);
        assertEquals(8, pluginFormPropertyTypes.getKVTuples().size());

    }

    public void testGenerateCreateDDL() {
        DataxMySQLWriter writer = new DataxMySQLWriter();
        writer.autoCreateTable = true;
        DataxReader.dataxReaderThreadLocal.set(new DataxReader(){
            @Override
            public <T extends ISelectedTab> List<T> getSelectedTabs() {
                return null;
            }

            @Override
            public IGroupChildTaskIterator getSubTasks() {
                return null;
            }

            @Override
            public String getTemplate() {
                return null;
            }
        });

        CreateTableSqlBuilder.CreateDDL ddl = writer.generateCreateDDL(getTabApplication((cols) -> {
            ISelectedTab.ColMeta col = new ISelectedTab.ColMeta();
            col.setPk(true);
            col.setName("id3");
            col.setType(DataXReaderColType.Long.dataType);
            cols.add(col);

            col = new ISelectedTab.ColMeta();
            col.setName("col4");
            col.setType(DataXReaderColType.STRING.dataType);
            cols.add(col);

            col = new ISelectedTab.ColMeta();
            col.setName("col5");
            col.setType(DataXReaderColType.STRING.dataType);
            cols.add(col);


            col = new ISelectedTab.ColMeta();
            col.setPk(true);
            col.setName("col6");
            col.setType(DataXReaderColType.STRING.dataType);
            cols.add(col);
        }));

        assertNotNull(ddl);
        // System.out.println(ddl);

        assertEquals(
                StringUtils.trimToEmpty(IOUtils.loadResourceFromClasspath(DataxMySQLWriter.class, "create-application-ddl.sql"))
                ,StringUtils.trimToEmpty( ddl.toString()));
    }

    private IDataxProcessor.TableMap getTabApplication(
            Consumer<List<ISelectedTab.ColMeta>>... colsProcess) {

        List<ISelectedTab.ColMeta> sourceCols = Lists.newArrayList();
        ISelectedTab.ColMeta col = new ISelectedTab.ColMeta();
        col.setPk(true);
        col.setName("user_id");
        col.setType(DataXReaderColType.Long.dataType);
        sourceCols.add(col);

        col = new ISelectedTab.ColMeta();
        col.setName("user_name");
        col.setType(DataXReaderColType.STRING.dataType);
        sourceCols.add(col);

        for (Consumer<List<ISelectedTab.ColMeta>> p : colsProcess) {
            p.accept(sourceCols);
        }
        IDataxProcessor.TableMap tableMap = new IDataxProcessor.TableMap(sourceCols);
        tableMap.setFrom("application");
        tableMap.setTo("application");
        //tableMap.setSourceCols(sourceCols);
        return tableMap;
    }


    public void testTempateGenerate() throws Exception {
        Optional<PluginExtraProps> extraProps = PluginExtraProps.load(DataxMySQLWriter.class);
        assertTrue("DataxMySQLWriter extraProps shall exist", extraProps.isPresent());
        IPluginContext pluginContext = EasyMock.createMock("pluginContext", IPluginContext.class);
        Context context = EasyMock.createMock("context", Context.class);
        EasyMock.expect(context.hasErrors()).andReturn(false);
        MySQLDataSourceFactory mysqlDs = new MySQLDataSourceFactory() {
            @Override
            public Connection getConnection(String jdbcUrl) throws SQLException {
                return null;
            }
        };

        mysqlDs.dbName = dbWriterName;
        mysqlDs.port = 3306;
        mysqlDs.encode = "utf8";
        mysqlDs.userName = "root";
        mysqlDs.password = "123456";
        mysqlDs.nodeDesc = "192.168.28.200";
        Descriptor.ParseDescribable<DataSourceFactory> desc = new Descriptor.ParseDescribable<>(mysqlDs);
        pluginContext.addDb(desc, dbWriterName, context, true);
        EasyMock.replay(pluginContext, context);

        DataSourceFactoryPluginStore dbStore = TIS.getDataBasePluginStore(new PostedDSProp(dbWriterName));

        assertTrue("save mysql db Config faild", dbStore.setPlugins(pluginContext, Optional.of(context), Collections.singletonList(desc)).success);


        DataxMySQLWriter mySQLWriter = new DataxMySQLWriter();
        mySQLWriter.dataXName = dataXName;
        mySQLWriter.writeMode = "replace";
        mySQLWriter.dbName = dbWriterName;
        mySQLWriter.template = DataxMySQLWriter.getDftTemplate();
        mySQLWriter.batchSize = 1001;
        mySQLWriter.preSql = "delete from test";
        mySQLWriter.postSql = "delete from test1";
        mySQLWriter.session = "set session sql_mode='ANSI'";
        validateConfigGenerate("mysql-datax-writer-assert.json", mySQLWriter);
        //  System.out.println(mySQLWriter.getTemplate());


        // 将非必须输入的值去掉再测试一遍
        mySQLWriter.batchSize = null;
        mySQLWriter.preSql = null;
        mySQLWriter.postSql = null;
        mySQLWriter.session = null;
        validateConfigGenerate("mysql-datax-writer-assert-without-option-val.json", mySQLWriter);

        mySQLWriter.preSql = " ";
        mySQLWriter.postSql = " ";
        mySQLWriter.session = " ";
        validateConfigGenerate("mysql-datax-writer-assert-without-option-val.json", mySQLWriter);


    }

    private void validateConfigGenerate(String assertFileName, DataxMySQLWriter mySQLWriter) throws IOException {
//        IDataxProcessor.TableMap tm = new IDataxProcessor.TableMap();
//        tm.setFrom("orderinfo");
//        tm.setTo("orderinfo_new");
//        tm.setSourceCols(Lists.newArrayList("col1", "col2", "col3").stream().map((c) -> {
//            ISelectedTab.ColMeta meta = new ISelectedTab.ColMeta();
//            meta.setName(c);
//            return meta;
//        }).collect(Collectors.toList()));
        Optional<IDataxProcessor.TableMap> tableMap = TestSelectedTabs.createTableMapper();
        IDataxContext subTaskCtx = mySQLWriter.getSubTask(tableMap);
        assertNotNull(subTaskCtx);

        RdbmsDataxContext mySQLDataxContext = (RdbmsDataxContext) subTaskCtx;
        assertEquals("\"`col1`\",\"`col2`\",\"`col3`\"", mySQLDataxContext.getColsQuotes());
        assertEquals(mysqlJdbcUrl, mySQLDataxContext.getJdbcUrl());
        assertEquals("123456", mySQLDataxContext.getPassword());
        assertEquals("orderinfo_new", mySQLDataxContext.tabName);
        assertEquals("root", mySQLDataxContext.getUsername());

        IDataxProcessor processor = EasyMock.mock("dataxProcessor", IDataxProcessor.class);
        IDataxGlobalCfg dataxGlobalCfg = EasyMock.mock("dataxGlobalCfg", IDataxGlobalCfg.class);

        IDataxReader dataxReader = EasyMock.mock("dataxReader", IDataxReader.class);

        EasyMock.expect(processor.getReader(null)).andReturn(dataxReader);
        EasyMock.expect(processor.getWriter(null)).andReturn(mySQLWriter);
        EasyMock.expect(processor.getDataXGlobalCfg()).andReturn(dataxGlobalCfg);
        EasyMock.replay(processor, dataxGlobalCfg, dataxReader);


        DataXCfgGenerator dataProcessor = new DataXCfgGenerator(null, "testDataXName", processor) {
            @Override
            public String getTemplateContent() {
                return mySQLWriter.getTemplate();
            }
        };

        String cfgResult = dataProcessor.generateDataxConfig(null, mySQLWriter, dataxReader, tableMap);

        JsonUtil.assertJSONEqual(this.getClass(), assertFileName, cfgResult, (m, e, a) -> {
            assertEquals(m, e, a);
        });
        EasyMock.verify(processor, dataxGlobalCfg, dataxReader);
    }


    public void testGetDftTemplate() {
        String dftTemplate = DataxMySQLWriter.getDftTemplate();
        assertNotNull("dftTemplate can not be null", dftTemplate);
    }

    public void testPluginExtraPropsLoad() throws Exception {
        Optional<PluginExtraProps> extraProps = PluginExtraProps.load(DataxMySQLWriter.class);
        assertTrue(extraProps.isPresent());
    }

//    public void testCreateDDLParser() {
//
//        SqlParser sqlParser = new SqlParser();
//        String createSql = IOUtils.loadResourceFromClasspath(TestDataxMySQLWriter.class, "sql/create-instancedetail.sql");
//        Expression statement = sqlParser.createExpression(createSql, new ParsingOptions());
//        Objects.requireNonNull(statement, "statement can not be null");
//        System.out.println(   SqlFormatter.formatSql(statement, Optional.empty()) );
//    }

}
