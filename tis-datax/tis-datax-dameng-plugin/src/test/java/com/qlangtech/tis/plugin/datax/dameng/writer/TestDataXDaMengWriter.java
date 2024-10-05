package com.qlangtech.tis.plugin.datax.dameng.writer;

import com.alibaba.citrus.turbine.Context;
import com.google.common.collect.Lists;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxGlobalCfg;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.datax.IGroupChildTaskIterator;
import com.qlangtech.tis.datax.impl.DataXCfgGenerator;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.extension.util.PluginExtraProps;
import com.qlangtech.tis.plugin.common.BasicTemplate;
import com.qlangtech.tis.plugin.datax.CreateTableSqlBuilder;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.datax.dameng.RdbmsDataxContext;
import com.qlangtech.tis.plugin.datax.dameng.ds.DaMengDataSourceFactory;
import com.qlangtech.tis.plugin.datax.dameng.ds.TestDaMengDataSourceFactory;
import com.qlangtech.tis.plugin.datax.test.TestSelectedTabs;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.DataSourceFactoryPluginStore;
import com.qlangtech.tis.plugin.ds.DataXReaderColType;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.PostedDSProp;
import com.qlangtech.tis.trigger.util.JsonUtil;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.lang.StringUtils;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/9/15
 */
public class TestDataXDaMengWriter {

    public static String damengJdbcUrl = "jdbc:dm://192.168.28.200:5236?schema=test";
    public static String dbWriterName = "test";

    public static String dataXName = "testDataXName";


    @Test
    public void testGenerateCreateDDL() {
        DaMengDataSourceFactory damengDS = TestDaMengDataSourceFactory.createDaMengDataSourceFactory();
        DataXDaMengWriter writer = new DataXDaMengWriter() {
            @Override
            public DaMengDataSourceFactory getDataSourceFactory() {
                return damengDS;
            }
        };
        writer.autoCreateTable = true;
        setPlaceholderReader();

        CreateTableSqlBuilder.CreateDDL ddl = writer.generateCreateDDL(TabApplicationCreator.getTabApplication((cols) -> {
            CMeta col = new CMeta();
            col.setPk(true);
            col.setName("id3");
            col.setType(DataXReaderColType.Long.dataType);
            cols.add(col);

            col = new CMeta();
            col.setName("col4");
            col.setType(DataXReaderColType.STRING.dataType);
            cols.add(col);

            col = new CMeta();
            col.setName("col5");
            col.setType(DataXReaderColType.STRING.dataType);
            cols.add(col);


            col = new CMeta();
            col.setPk(true);
            col.setName("col6");
            col.setType(DataXReaderColType.STRING.dataType);
            cols.add(col);
        }));

        Assert.assertNotNull(ddl);
        // System.out.println(ddl);

        Assert.assertEquals(
                StringUtils.trimToEmpty(IOUtils.loadResourceFromClasspath(DataXDaMengWriter.class, "create-application-ddl.sql"))
                , StringUtils.trimToEmpty(ddl.getDDLScript().toString()));
    }

    public static void setPlaceholderReader() {
        DataxReader.dataxReaderThreadLocal.set(new DataxReader() {
            @Override
            public void startScanDependency() {

            }
            @Override
            public <T extends ISelectedTab> List<T> getSelectedTabs() {
                return null;
            }

            @Override
            public IGroupChildTaskIterator getSubTasks(Predicate<ISelectedTab> filter) {
                return null;
            }

            @Override
            public String getTemplate() {
                return null;
            }
        });
    }

    private IDataxProcessor.TableMap getTabApplication(Consumer<List<CMeta>>... colsProcess) {
        String tableName = "application";
        SelectedTab tab = new SelectedTab();
        tab.name = tableName;

        List<CMeta> sourceCols = Lists.newArrayList();
        CMeta col = new CMeta();
        col.setPk(true);
        col.setName("user_id");
        col.setType(DataXReaderColType.Long.dataType);
        tab.primaryKeys = Lists.newArrayList(col.getName());
        sourceCols.add(col);

        col = new CMeta();
        col.setName("user_name");
        col.setType(DataXReaderColType.STRING.dataType);
        sourceCols.add(col);

        for (Consumer<List<CMeta>> p : colsProcess) {
            p.accept(sourceCols);
        }
        tab.cols.addAll(sourceCols);
        IDataxProcessor.TableMap tableMap = new IDataxProcessor.TableMap(tab);
        tableMap.setFrom(tableName);
        tableMap.setTo("application_alias");
        //tableMap.setSourceCols(sourceCols);
        return tableMap;
    }

    @Test
    public void testTempateGenerate() throws Exception {
        Optional<PluginExtraProps> extraProps = PluginExtraProps.load(DataXDaMengWriter.class);
        Assert.assertTrue("DataxDaMengWriter extraProps shall exist", extraProps.isPresent());
        IPluginContext pluginContext = EasyMock.createMock("pluginContext", IPluginContext.class);
        Context context = EasyMock.createMock("context", Context.class);
        EasyMock.expect(context.hasErrors()).andReturn(false);
        DaMengDataSourceFactory mysqlDs = TestDaMengDataSourceFactory.createDaMengDataSourceFactory();
//        new DaMengDataSourceFactory() {
//            @Override
//            public JDBCConnection getConnection(String jdbcUrl) throws SQLException {
//                return null;
//            }
//        };
//
//        mysqlDs.dbName = dbWriterName;
//        mysqlDs.port = 3306;
//        mysqlDs.encode = "utf8";
//        mysqlDs.userName = "root";
//        mysqlDs.password = "123456";
//        mysqlDs.nodeDesc = "192.168.28.200";
        Descriptor.ParseDescribable<DataSourceFactory> desc = new Descriptor.ParseDescribable<>(mysqlDs);
        pluginContext.addDb(desc, dbWriterName, context, true);
        EasyMock.replay(pluginContext, context);

        DataSourceFactoryPluginStore dbStore = TIS.getDataSourceFactoryPluginStore(PostedDSProp.parse(dbWriterName));

        Assert.assertTrue("save mysql db Config faild"
                , dbStore.setPlugins(pluginContext, Optional.of(context), Collections.singletonList(desc)).success);


        DataXDaMengWriter damengWriter = new DataXDaMengWriter();
        damengWriter.dataXName = dataXName;
        // mySQLWriter.writeMode = "replace";
        damengWriter.dbName = dbWriterName;
        damengWriter.template = DataXDaMengWriter.getDftTemplate();
        damengWriter.batchSize = 1001;
        damengWriter.preSql = "delete from test";
        damengWriter.postSql = "delete from test1";
        damengWriter.session = "set session sql_mode='ANSI'";
        validateConfigGenerate("dameng-datax-writer-assert.json", damengWriter);
        //  System.out.println(mySQLWriter.getTemplate());


        // 将非必须输入的值去掉再测试一遍
        damengWriter.batchSize = null;
        damengWriter.preSql = null;
        damengWriter.postSql = null;
        damengWriter.session = null;
        validateConfigGenerate("dameng-datax-writer-assert-without-option-val.json", damengWriter);

        damengWriter.preSql = " ";
        damengWriter.postSql = " ";
        damengWriter.session = " ";
        validateConfigGenerate("dameng-datax-writer-assert-without-option-val.json", damengWriter);


    }

    private void validateConfigGenerate(String assertFileName, DataXDaMengWriter mySQLWriter) throws IOException {

        Optional<IDataxProcessor.TableMap> tableMap = TestSelectedTabs.createTableMapper();
        IDataxContext subTaskCtx = mySQLWriter.getSubTask(tableMap,Optional.empty());
        Assert.assertNotNull(subTaskCtx);

        RdbmsDataxContext mySQLDataxContext = (RdbmsDataxContext) subTaskCtx;
        Assert.assertEquals("\"col1\",\"col2\",\"col3\"", mySQLDataxContext.getColsQuotes());
        Assert.assertEquals("jdbc:dm://192.168.28.201:30236?schema=TIS", mySQLDataxContext.getJdbcUrl());
        Assert.assertEquals("SYSDBA001", mySQLDataxContext.getPassword());
        Assert.assertEquals("orderinfo_new", mySQLDataxContext.getTabName());
        Assert.assertEquals("SYSDBA", mySQLDataxContext.getUsername());

        IDataxProcessor processor = EasyMock.mock("dataxProcessor", IDataxProcessor.class);
        IDataxGlobalCfg dataxGlobalCfg = EasyMock.mock("dataxGlobalCfg", IDataxGlobalCfg.class);

        EasyMock.expect(dataxGlobalCfg.getTemplate()).andReturn(DataXDaMengWriter.getDftTemplate()).anyTimes();

        IDataxReader dataxReader = EasyMock.mock("dataxReader", IDataxReader.class);
        // EasyMock.expect(dataxReader.getTemplate()).andReturn(StringUtils.EMPTY);
//        EasyMock.expect(processor.getReader(null)).andReturn(dataxReader);
//        EasyMock.expect(processor.getWriter(null)).andReturn(mySQLWriter);
        EasyMock.expect(processor.getDataXGlobalCfg()).andReturn(dataxGlobalCfg).anyTimes();
        EasyMock.replay(processor, dataxGlobalCfg, dataxReader);


        String cfgResult = generateDataXCfg(mySQLWriter, tableMap);

        JsonUtil.assertJSONEqual(this.getClass(), assertFileName, cfgResult, (m, e, a) -> {
            Assert.assertEquals(m, e, a);
        });
        EasyMock.verify(processor, dataxGlobalCfg, dataxReader);
    }

    public static String generateDataXCfg(DataXDaMengWriter mySQLWriter, Optional<IDataxProcessor.TableMap> tableMap) throws IOException {
        DataXCfgGenerator dataProcessor = BasicTemplate.createMockDataXCfgGenerator(DataXDaMengWriter.getDftTemplate());

        String cfgResult = dataProcessor.generateDataxConfig(null, mySQLWriter, null, tableMap);
        return cfgResult;
    }

}
