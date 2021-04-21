package com.qlangtech.tis.plugin.datax;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSON;
import com.google.common.collect.Lists;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.datax.IDataxWriter;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.PluginFormProperties;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.extension.impl.RootFormProperties;
import com.qlangtech.tis.extension.util.PluginExtraProps;
import com.qlangtech.tis.plugin.BasicTest;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.DataSourceFactoryPluginStore;
import com.qlangtech.tis.plugin.ds.PostedDSProp;
import com.qlangtech.tis.plugin.ds.mysql.MySQLDataSourceFactory;
import com.qlangtech.tis.util.IPluginContext;
import org.easymock.EasyMock;

import java.io.IOException;
import java.util.Collections;
import java.util.Optional;

/**
 * @author: baisui 百岁
 * @create: 2021-04-15 16:10
 **/
public class TestDataxMySQLWriter extends BasicTest {
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
        MySQLDataSourceFactory mysqlDs = new MySQLDataSourceFactory();

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


    }

    private void validateConfigGenerate(String assertFileName, DataxMySQLWriter mySQLWriter) throws IOException {
        IDataxProcessor.TableMap tm = new IDataxProcessor.TableMap();
        tm.setFrom("orderinfo");
        tm.setTo("orderinfo_new");
        tm.setSourceCols(Lists.newArrayList("col1", "col2", "col3"));
        Optional<IDataxProcessor.TableMap> tableMap = Optional.of(tm);
        IDataxContext subTaskCtx = mySQLWriter.getSubTask(tableMap);
        assertNotNull(subTaskCtx);

        MySQLDataxContext mySQLDataxContext = (MySQLDataxContext) subTaskCtx;
        assertEquals("\"`col1`\",\"`col2`\",\"`col3`\"", mySQLDataxContext.getColsQuotes());
        assertEquals("jdbc:mysql://192.168.28.200:3306/baisuitestWriterdb?useUnicode=yes&characterEncoding=utf8", mySQLDataxContext.getJdbcUrl());
        assertEquals("123456", mySQLDataxContext.getPassword());
        assertEquals("orderinfo_new", mySQLDataxContext.tabName);
        assertEquals("root", mySQLDataxContext.getUsername());


        MockDataxProcessor dataProcessor = new MockDataxProcessor(mySQLWriter);

        System.out.println(dataProcessor.generateDataxConfig(null, Optional.of(tm)));

        assertEquals(JSON.parseObject(IOUtils.loadResourceFromClasspath(this.getClass(), assertFileName)).toJSONString(),
                JSON.parseObject(dataProcessor.generateDataxConfig(null, Optional.of(tm))).toJSONString()
        );
    }

    public static class MockDataxProcessor extends DataxProcessor {
        private final IDataxWriter dataxWriter;

        public MockDataxProcessor(IDataxWriter dataxWriter) {
            this.dataxWriter = dataxWriter;
        }

        @Override
        public IDataxReader getReader() {
            throw new UnsupportedOperationException();
        }

        @Override
        public String getTemplateContent() {
            return dataxWriter.getTemplate();
        }

        @Override
        public String generateDataxConfig(IDataxContext readerContext, Optional<TableMap> tableMap) throws IOException {
            return super.generateDataxConfig(readerContext, tableMap);
        }

        @Override
        protected int getChannel() {
            return 0;
        }

        @Override
        protected int getErrorLimitCount() {
            return 0;
        }

        @Override
        protected int getErrorLimitPercentage() {
            return 0;
        }


        @Override
        public IDataxWriter getWriter() {
            return this.dataxWriter;
        }


    }
}
