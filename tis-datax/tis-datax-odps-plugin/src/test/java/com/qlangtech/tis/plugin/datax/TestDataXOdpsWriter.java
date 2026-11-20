package com.qlangtech.tis.plugin.datax;

import com.google.common.collect.Lists;
import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxGlobalCfg;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.datax.SourceColMetaGetter;
import com.qlangtech.tis.datax.TimeFormat;
import com.qlangtech.tis.datax.impl.DataXCfgGenerator;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.extension.util.PluginExtraProps;
import com.qlangtech.tis.plugin.aliyun.IAliyunAccessKey;
import com.qlangtech.tis.plugin.datax.odps.OdpsDataSourceFactory;
import com.qlangtech.tis.plugin.datax.test.TestSelectedTabs;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.ds.DataXReaderColType;
import com.qlangtech.tis.trigger.util.JsonUtil;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.mockito.Mockito;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;

import static org.junit.Assert.*;
import org.junit.Test;
/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-08 11:35
 **/
public class TestDataXOdpsWriter {
    @Test
    public void testGetDftTemplate() {
        String dftTemplate = DataXOdpsWriter.getDftTemplate();
        assertNotNull("dftTemplate can not be null", dftTemplate);
    }

    @Test
    public void testPluginExtraPropsLoad() throws Exception {
        Optional<PluginExtraProps> extraProps = PluginExtraProps.load(DataXOdpsWriter.class);
        assertTrue(extraProps.isPresent());
    }


    @Test
    public void testTempateGenerate() throws Exception {
        Optional<PluginExtraProps> extraProps = PluginExtraProps.load(DataXOdpsWriter.class);
        Assert.assertTrue("DataXOdpsWriter extraProps shall exist", extraProps.isPresent());
        //IPluginContext pluginContext = Mockito.mock(IPluginContext.class, Mockito.withSettings().name("pluginContext"));
        // Context context = Mockito.mock(Context.class, Mockito.withSettings().name("context"));
        // Mockito.when(context.hasErrors()).thenReturn(false);

        final DataXOdpsWriter odpsWriter = createDataXOdpsWriter();
        OdpsDataSourceFactory dsFactory = odpsWriter.getDataSourceFactory();
        validateConfigGenerate("odps-datax-writer-assert.json", dsFactory, dsFactory.getAccessKey(), odpsWriter);
        //  System.out.println(mySQLWriter.getTemplate());


    }

    private DataXOdpsWriter createDataXOdpsWriter() {
        AccessToken access = new AccessToken();


        OdpsDataSourceFactory endpoint = new OdpsDataSourceFactory();
        endpoint.name = "odps1";
        endpoint.odpsServer = "http://sxxx/api";
        endpoint.tunnelServer = "http://xxx";
        endpoint.project = "chinan_test";
        endpoint.authToken = access;

        DataXOdpsWriter odpsWriter = new DataXOdpsWriter() {
            @Override
            public OdpsDataSourceFactory getDataSourceFactory() {
                return endpoint;
            }
        };
        odpsWriter.truncate = true;
        odpsWriter.template = DataXOdpsWriter.getDftTemplate();
        odpsWriter.dbName = "odpsEndpoint";
        odpsWriter.partitionFormat = TimeFormat.yyyyMMddHHmmss.name();
        return odpsWriter;
    }


    private void validateConfigGenerate(String assertFileName, OdpsDataSourceFactory endpoint, IAliyunAccessKey access, DataXOdpsWriter odpsWriter) throws IOException {

        Optional<IDataxProcessor.TableMap> tableMap = TestSelectedTabs.createTableMapper();
        IDataxProcessor.TableMap tab = tableMap.get();
        IDataxContext subTaskCtx = odpsWriter.getSubTask(tableMap, Optional.empty());
        Assert.assertNotNull(subTaskCtx);

        DataXOdpsWriter.OdpsContext odpsWriterContext = (DataXOdpsWriter.OdpsContext) subTaskCtx;

        Assert.assertTrue(CollectionUtils.isEqualCollection(
                Lists.newArrayList("col1", "col2", "col3"), odpsWriterContext.getColumn()));

        Assert.assertEquals(access.getAccessKeyId(), odpsWriterContext.getAccessId());
        Assert.assertEquals(access.getAccessKeySecret(), odpsWriterContext.getAccessKey());
        Assert.assertEquals(tab.getTo(), odpsWriterContext.getTable());
        Assert.assertEquals(endpoint.project, odpsWriterContext.getProject());
        Assert.assertEquals(odpsWriter.truncate, odpsWriterContext.isTruncate());

        IDataxProcessor processor = Mockito.mock(IDataxProcessor.class, Mockito.withSettings().name("dataxProcessor"));
        IDataxGlobalCfg dataxGlobalCfg = Mockito.mock(IDataxGlobalCfg.class, Mockito.withSettings().name("dataxGlobalCfg"));


        Mockito.when(dataxGlobalCfg.getTemplate()).thenReturn(DataXOdpsWriter.getDftTemplate());

        IDataxReader dataxReader = Mockito.mock(IDataxReader.class, Mockito.withSettings().name("dataxReader"));
        Mockito.when(dataxReader.getTemplate()).thenReturn("test");
        Mockito.when(processor.getReader(null)).thenReturn(dataxReader);
        Mockito.when(processor.getWriter(null)).thenReturn(odpsWriter);
        Mockito.when(processor.getDataXGlobalCfg()).thenReturn(dataxGlobalCfg);


        DataXCfgGenerator dataProcessor = new DataXCfgGenerator(null, "testDataXName", processor) {
        };

        String cfgResult = dataProcessor.generateDataxConfig(null, odpsWriter, dataxReader, tableMap);

        JsonUtil.assertJSONEqual(this.getClass(), assertFileName, cfgResult, (m, e, a) -> {
            Assert.assertEquals(m, e, a);
        });
        Mockito.verify(processor).getDataXGlobalCfg();
        Mockito.verify(dataxGlobalCfg).getTemplate();
        Mockito.verify(dataxReader).getTemplate();
    }


    @Test
    public void testGenerateCreateDDL() {

        final DataXOdpsWriter writer = createDataXOdpsWriter();
        Assert.assertFalse(writer.isGenerateCreateDDLSwitchOff());
        CreateTableSqlBuilder.CreateDDL ddl = writer.generateCreateDDL(SourceColMetaGetter.getNone(), getTabApplication((cols) -> {
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
        }), Optional.empty());

        assertNotNull(ddl);

        assertEquals(
                StringUtils.trimToEmpty(IOUtils.loadResourceFromClasspath(DataXOdpsWriter.class, "odps-create-application-ddl.sql"))
                , ddl.getDDLScript());

    }

    protected IDataxProcessor.TableMap getTabApplication(
            Consumer<List<CMeta>>... colsProcess) {

        List<CMeta> sourceCols = Lists.newArrayList();
        CMeta col = new CMeta();
        col.setPk(true);
        col.setName("user_id");
        col.setType(DataXReaderColType.Long.dataType);
        sourceCols.add(col);

        col = new CMeta();
        col.setName("user_name");
        col.setType(DataXReaderColType.STRING.dataType);
        sourceCols.add(col);

        for (Consumer<List<CMeta>> p : colsProcess) {
            p.accept(sourceCols);
        }
        IDataxProcessor.TableMap tableMap = new IDataxProcessor.TableMap(sourceCols);
        tableMap.setFrom("application");
        tableMap.setTo("application");
        //tableMap.setSourceCols(sourceCols);
        return tableMap;
    }
}
