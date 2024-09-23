package com.qlangtech.tis.plugin.datax.dameng.reader;

import com.alibaba.datax.common.spi.IDataXCfg;
import com.alibaba.datax.common.util.Configuration;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.extension.util.PluginExtraProps;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.offline.DataxUtils;
import com.qlangtech.tis.plugin.common.ReaderTemplate;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.datax.dameng.ds.DaMengDataSourceFactory;
import com.qlangtech.tis.plugin.datax.dameng.ds.TestDaMengDataSourceFactory;
import com.qlangtech.tis.plugin.datax.dameng.writer.TestDataXDaMengWriter;
import com.qlangtech.tis.plugin.datax.test.TestSelectedTabs;
import com.qlangtech.tis.plugin.ds.DataDumpers;
import com.qlangtech.tis.plugin.ds.IDataSourceDumper;
import com.qlangtech.tis.plugin.ds.NoneSplitTableStrategy;
import com.qlangtech.tis.plugin.ds.SplitTableStrategy;
import com.qlangtech.tis.plugin.ds.SplitableTableInDB;
import com.qlangtech.tis.plugin.ds.TISTable;
import com.qlangtech.tis.plugin.ds.split.SplitTableStrategyUtils;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.apache.commons.io.FileUtils;
import org.easymock.EasyMock;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/9/15
 */
public class TestDataXDaMengReader {
    @Rule
    public TemporaryFolder folder = new TemporaryFolder();
    public static final String userName = "SYSDBA";
    public static final String password = "SYSDBA001";


    @Test
    public void testTempateGenerate() throws Exception {

        Optional<PluginExtraProps> extraProps = PluginExtraProps.load(DataXDaMengReader.class);
        assertTrue("DataXDaMengReader extraProps shall exist", extraProps.isPresent());

        DaMengDataSourceFactory datasource = EasyMock.createMock("damengDSFactory", DaMengDataSourceFactory.class);
        datasource.splitTableStrategy = new NoneSplitTableStrategy();
        datasource.name = "dameng_ds";

        SplitableTableInDB tabsInDB
                = new SplitableTableInDB(datasource, SplitTableStrategy.PATTERN_PHYSICS_TABLE, false);
        tabsInDB.add(TestDataXDaMengWriter.damengJdbcUrl, TestSelectedTabs.tabNameOrderDetail + "_01");
        tabsInDB.add(TestDataXDaMengWriter.damengJdbcUrl, TestSelectedTabs.tabNameOrderDetail + "_02");
        datasource.fillTableInDB(tabsInDB);
        EasyMock.expect(datasource.createTableInDB()).andReturn(tabsInDB).times(1);
        // EasyMock.expect(datasource.getTablesInDB()).andReturn(tabsInDB).times(1);

        EasyMock.expect(datasource.getPassword()).andReturn(password).anyTimes();
        EasyMock.expect(datasource.getUserName()).andReturn(userName).anyTimes();
        IDataSourceDumper dataDumper = EasyMock.createMock(TestSelectedTabs.tabNameOrderDetail + "TableDumper", IDataSourceDumper.class);
        EasyMock.expect(dataDumper.getDbHost()).andReturn(TestDataXDaMengWriter.damengJdbcUrl).anyTimes();
// int index, String key, int type, boolean pk
        TISTable targetTable = new TISTable();
        targetTable.setTableName(TestSelectedTabs.tabNameOrderDetail);

        EasyMock.expect(datasource.getTableMetadata(false, EntityName.parse(TestSelectedTabs.tabNameOrderDetail)))
                .andReturn(TestSelectedTabs.tabColsMetaOrderDetail).anyTimes();

        EasyMock.expect(datasource.getDataDumpers(targetTable)).andDelegateTo(new DaMengDataSourceFactory() {
            @Override
            public DataDumpers getDataDumpers(TISTable table) {
                return new DataDumpers(1, Collections.singletonList(dataDumper).iterator());
            }
        }).anyTimes();

        EasyMock.replay(datasource, dataDumper);


        DataXDaMengReader damengReader = new DataXDaMengReader() {
            @Override
            public DaMengDataSourceFactory getDataSourceFactory() {
                return datasource;
            }

            @Override
            public Class<?> getOwnerClass() {
                return DataXDaMengReader.class;
            }
        };
        damengReader.dataXName = BasicRDBMSDataXReaderTest.dataXName;
        damengReader.template = DataXDaMengReader.getDftTemplate();


        final List<SelectedTab> selectedTabs = TestSelectedTabs.createSelectedTabs(1);

        damengReader.setSelectedTabs(selectedTabs);
        //校验证列和 where条件都设置的情况
        // valiateReaderCfgGenerate("mysql-datax-reader-assert.json", processor, mySQLReader);

        ReaderTemplate.validateDataXReader("dameng-datax-reader-assert.json", BasicRDBMSDataXReaderTest.dataXName, damengReader);

        damengReader.setSelectedTabs(TestSelectedTabs.createSelectedTabs(1));

        ReaderTemplate.validateDataXReader("dameng-datax-reader-assert-without-option-val.json", BasicRDBMSDataXReaderTest.dataXName, damengReader);

        // DefaultSplitTableStrategy splitTableStrategy = SplitTableStrategyUtils.createSplitTableStrategy();
        datasource.splitTableStrategy = SplitTableStrategyUtils.createSplitTableStrategy();
        ReaderTemplate.validateDataXReader("dameng-datax-reader-assert-split-tabs.json", BasicRDBMSDataXReaderTest.dataXName, damengReader);

        EasyMock.verify(datasource, dataDumper);
    }


    @Test
    public void testGetDftTemplate() throws Exception {
        String dftTemplate = DataXDaMengReader.getDftTemplate();
        assertNotNull("dftTemplate can not be null", dftTemplate);
    }

    @Test
    public void testRealRead() throws Exception {

        DaMengDataSourceFactory daMengDataSource = TestDaMengDataSourceFactory.createDaMengDataSourceFactory();

        DataXDaMengReader dataxReader = new DataXDaMengReader();
        File dataxReaderResult = folder.newFile("mysql-datax-reader-result.txt");

        Configuration readerConf = IOUtils.loadResourceFromClasspath(
                dataxReader.getClass(), "mysql-datax-reader-test-cfg.json", true, (writerJsonInput) -> {
                    return Configuration.from(writerJsonInput);
                });
        readerConf.set("parameter.connection[0].jdbcUrl[0]", daMengDataSource.getJdbcUrls().get(0));
        readerConf.set(IDataXCfg.connectKeyParameter
                + "." + DataxUtils.DATASOURCE_FACTORY_IDENTITY, daMengDataSource.identityValue());
        ReaderTemplate.realExecute(BasicRDBMSDataXReaderTest.dataXName, readerConf, dataxReaderResult, dataxReader);
        System.out.println(FileUtils.readFileToString(dataxReaderResult, TisUTF8.get()));

    }

}
