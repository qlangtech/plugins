/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.tis.plugin.datax;

import com.alibaba.citrus.turbine.Context;
import com.google.common.collect.Lists;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxGlobalCfg;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.ISelectedTab;
import com.qlangtech.tis.datax.impl.DataXCfgGenerator;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.PluginFormProperties;
import com.qlangtech.tis.extension.impl.RootFormProperties;
import com.qlangtech.tis.extension.util.PluginExtraProps;
import com.qlangtech.tis.plugin.BasicTest;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.DataSourceFactoryPluginStore;
import com.qlangtech.tis.plugin.ds.PostedDSProp;
import com.qlangtech.tis.plugin.ds.mysql.MySQLDataSourceFactory;
import com.qlangtech.tis.trigger.util.JsonUtil;
import com.qlangtech.tis.util.IPluginContext;
import org.easymock.EasyMock;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Collections;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author: baisui 百岁
 * @create: 2021-04-15 16:10
 **/
public class TestDataxMySQLWriter extends BasicTest {
    public static String mysqlJdbcUrl = "jdbc:mysql://192.168.28.200:3306/baisuitestWriterdb?useUnicode=yes&characterEncoding=utf8";
    public static String dbWriterName = "baisuitestWriterdb";

    public void testFieldCount() throws Exception {
        DataxMySQLWriter mySQLWriter = new DataxMySQLWriter();
        Descriptor<DataxWriter> descriptor = mySQLWriter.getDescriptor();
        PluginFormProperties pluginFormPropertyTypes = descriptor.getPluginFormPropertyTypes();

        assertTrue(pluginFormPropertyTypes instanceof RootFormProperties);
        assertEquals(7, pluginFormPropertyTypes.getKVTuples().size());

    }

    public void testTempateGenerate() throws Exception {
        Optional<PluginExtraProps> extraProps = PluginExtraProps.load(DataxMySQLWriter.class);
        assertTrue("DataxMySQLWriter extraProps shall exist", extraProps.isPresent());
        IPluginContext pluginContext = EasyMock.createMock("pluginContext", IPluginContext.class);
        Context context = EasyMock.createMock("context", Context.class);
        EasyMock.expect(context.hasErrors()).andReturn(false);
        MySQLDataSourceFactory mysqlDs = new MySQLDataSourceFactory(){
            @Override
            protected Connection getConnection(String jdbcUrl, String username, String password) throws SQLException {
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

        assertTrue("save mysql db Config faild", dbStore.setPlugins(pluginContext, Optional.of(context), Collections.singletonList(desc)));


        DataxMySQLWriter mySQLWriter = new DataxMySQLWriter();
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
        IDataxProcessor.TableMap tm = new IDataxProcessor.TableMap();
        tm.setFrom("orderinfo");
        tm.setTo("orderinfo_new");
        tm.setSourceCols(Lists.newArrayList("col1", "col2", "col3").stream().map((c) -> {
            ISelectedTab.ColMeta meta = new ISelectedTab.ColMeta();
            meta.setName(c);
            return meta;
        }).collect(Collectors.toList()));
        Optional<IDataxProcessor.TableMap> tableMap = Optional.of(tm);
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

        EasyMock.expect(processor.getWriter(null)).andReturn(mySQLWriter);
        EasyMock.expect(processor.getDataXGlobalCfg()).andReturn(dataxGlobalCfg);
        EasyMock.replay(processor, dataxGlobalCfg);


        DataXCfgGenerator dataProcessor = new DataXCfgGenerator(null, "testDataXName", processor) {
            @Override
            public String getTemplateContent() {
                return mySQLWriter.getTemplate();
            }
        };

        String cfgResult = dataProcessor.generateDataxConfig(null, Optional.of(tm));

        JsonUtil.assertJSONEqual(this.getClass(), assertFileName, cfgResult, (m, e, a) -> {
            assertEquals(m, e, a);
        });
        EasyMock.verify(processor, dataxGlobalCfg);
    }


    public void testGetDftTemplate() {
        String dftTemplate = DataxMySQLWriter.getDftTemplate();
        assertNotNull("dftTemplate can not be null", dftTemplate);
    }

    public void testPluginExtraPropsLoad() throws Exception {
        Optional<PluginExtraProps> extraProps = PluginExtraProps.load(DataxMySQLWriter.class);
        assertTrue(extraProps.isPresent());
    }

}
