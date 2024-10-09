package com.qlangtech.tis.plugin.datax.dameng.reader;

import com.alibaba.datax.common.spi.IDataXCfg;
import com.alibaba.datax.common.util.Configuration;
import com.google.common.collect.Lists;
import com.qlangtech.plugins.incr.flink.cdc.TestSelectedTab;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.datax.DataXCfgFile;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.offline.DataxUtils;
import com.qlangtech.tis.plugin.common.ReaderTemplate;
import com.qlangtech.tis.plugin.datax.AbstractCreateTableSqlBuilder;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsReader;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsWriter;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.DSKey;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.DataSourceFactoryPluginStore;
import com.qlangtech.tis.plugin.ds.IDataSourceFactoryGetter;
import org.apache.commons.io.FileUtils;
import org.easymock.EasyMock;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.io.File;
import java.util.Optional;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/9/18
 */
@RunWith(Parameterized.class)
public abstract class BasicRDBMSDataXReaderTest
        <READER extends BasicDataXRdbmsReader<DS>, WRITER extends BasicDataXRdbmsWriter<DS>, DS extends BasicDataSourceFactory> {

    public static final String dataXName = "dataXName";
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    private final String dataXReaderConfig;
    private WRITER dataXWriter;

    public BasicRDBMSDataXReaderTest(String dataXReaderConfig, Class<WRITER> clazz) {
        this.dataXReaderConfig = dataXReaderConfig;
        try {
            this.dataXWriter = clazz.newInstance();
            this.dataXWriter.dbName = "dbName";
            this.dataXWriter.autoCreateTable = true;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testRealRead() throws Exception {

        DS dsFactory = createDataSourceFactory();
        TIS.dsFactoryPluginStoreGetter = (p) -> {
            DSKey key = new DSKey(TIS.DB_GROUP_NAME, p, DataSourceFactory.class);
            return new DataSourceFactoryPluginStore(key, false) {
                @Override
                public DataSourceFactory getPlugin() {
                    return dsFactory;
                }
            };
        };
        READER dataxReader = createDataXReader(dsFactory);
        SelectedTab tab =
                TestSelectedTab.load(folder, BasicRDBMSDataXReaderTest.class, "datax-reader-cfg-selected-tabs-full-types.xml");

        DataxProcessor processor = EasyMock.createMock("processor", DataxProcessor.class);

        EasyMock.expect(processor.getReader(null)).andReturn(dataxReader).times(2);

        final File ddlDir = folder.newFolder("ddlDir");
        EasyMock.expect(processor.getDataxCreateDDLDir(null)).andReturn(ddlDir);
        AbstractCreateTableSqlBuilder.CreateDDL createDDL
                = this.dataXWriter.generateCreateDDL(new IDataxProcessor.TableMap(tab), Optional.empty());

        FileUtils.write(new File(ddlDir
                , tab.getName() + DataXCfgFile.DATAX_CREATE_DDL_FILE_NAME_SUFFIX), createDDL.getDDLScript());


        EasyMock.replay(processor);

        dsFactory.visitAllConnection((conn) -> {

            try {
                conn.execute("drop table if exists " + dsFactory.getEscapedEntity(tab.getName()));
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            // initialize table
            BasicDataXRdbmsWriter.process(dataXName, processor, new IDataSourceFactoryGetter() {
                @Override
                public DataSourceFactory getDataSourceFactory() {
                    return dsFactory;
                }

                @Override
                public Integer getRowFetchSize() {
                    return 1;
                }
            }, dataXWriter, conn, tab.getName());

            // insert rows into table

        });


        // DataxProcessor dataxProcessor = EasyMock.createMock("dataXProcessor", DataxProcessor.class);
        DataxProcessor.processorGetter = (name) -> processor;

        dataxReader.template = dataXReaderConfig;
        dataxReader.selectedTabs = Lists.newArrayList(tab);
        File dataxReaderResult = folder.newFile("datax-reader-result.txt");

        Configuration readerConf = Configuration.from(ReaderTemplate.generateReaderCfg(dataxReader));

        readerConf.set("parameter.connection[0].jdbcUrl[0]", dsFactory.getJdbcUrls().get(0));
        readerConf.set(IDataXCfg.connectKeyParameter
                + "." + DataxUtils.DATASOURCE_FACTORY_IDENTITY, dsFactory.identityValue());

        ReaderTemplate.realExecute(TestDataXDaMengReaderReal.dataXName, readerConf, dataxReaderResult, dataxReader);
        System.out.println(FileUtils.readFileToString(dataxReaderResult, TisUTF8.get()));

        EasyMock.verify(processor);

    }


    protected abstract READER createDataXReader(DS daMengDataSource);

    protected abstract DS createDataSourceFactory();

}
