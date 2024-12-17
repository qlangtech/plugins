package com.qlangtech.tis.plugin.datax.dameng.writer;

import com.alibaba.datax.plugin.writer.hdfswriter.HdfsColMeta;
import com.google.common.collect.Lists;
import com.qlangtech.tis.datax.DataXCfgFile;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.SourceColMetaGetter;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.plugin.common.DataXCfgJson;
import com.qlangtech.tis.plugin.common.WriterTemplate;
import com.qlangtech.tis.plugin.datax.CreateTableSqlBuilder;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.datax.common.AutoCreateTable;
import com.qlangtech.tis.plugin.datax.dameng.ds.DaMengDataSourceFactory;
import com.qlangtech.tis.plugin.datax.dameng.ds.TestDaMengDataSourceFactory;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.JDBCTypes;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.util.Optional;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/9/15
 */
public class TestDataXDaMengWriterReal {

    @Rule
    public TemporaryFolder folder = new TemporaryFolder();

    @Test
    public void testRealDump() throws Exception {

        final String targetTableName = "customer_order_relation";
        String testDataXName = "mysql_dameng";

        final DataXDaMengWriter writer = getDamengWriter();
        writer.dataXName = testDataXName;
        SelectedTab tab = new SelectedTab();
        tab.name = targetTableName;
        // List<IColMetaGetter> colMetas = Lists.newArrayList();

//                "customerregister_id",
//                "waitingorder_id",
//                "kind",
//                "create_time",
//                "last_ver"
        // DataType
        HdfsColMeta cmeta = null;
        // String colName, Boolean nullable, Boolean pk, DataType dataType
        tab.primaryKeys = Lists.newArrayList();
        cmeta = new HdfsColMeta("customerregister_id", false
                , true, DataType.createVarChar(150));
        tab.cols.add(IDataxProcessor.TableMap.getCMeta(cmeta));
        tab.primaryKeys.add(cmeta.getName());

        cmeta = new HdfsColMeta("waitingorder_id", false, true
                , DataType.createVarChar(150));
        tab.cols.add(IDataxProcessor.TableMap.getCMeta(cmeta));
        tab.primaryKeys.add(cmeta.getName());

        cmeta = new HdfsColMeta("kind"
                , true, false, DataType.getType(JDBCTypes.BIGINT));
        tab.cols.add(IDataxProcessor.TableMap.getCMeta(cmeta));

        cmeta = new HdfsColMeta("create_time"
                , true, false, DataType.getType(JDBCTypes.BIGINT));
        tab.cols.add(IDataxProcessor.TableMap.getCMeta(cmeta));

        cmeta = new HdfsColMeta("last_ver"
                , true, false, DataType.getType(JDBCTypes.BIGINT));
        tab.cols.add(IDataxProcessor.TableMap.getCMeta(cmeta));

        IDataxProcessor.TableMap tabMap = new IDataxProcessor.TableMap(tab); //IDataxProcessor.TableMap.create(targetTableName, colMetas);
        TestDataXDaMengWriter.setPlaceholderReader();

        DataXCfgJson wjson = DataXCfgJson.content(TestDataXDaMengWriter.generateDataXCfg(writer, Optional.of(tabMap)));
        CreateTableSqlBuilder.CreateDDL ddl = writer.generateCreateDDL(SourceColMetaGetter.getNone(), tabMap, Optional.empty());

        DataxProcessor dataXProcessor = EasyMock.mock("dataXProcessor", DataxProcessor.class);
        File createDDLDir = folder.newFolder();// new File(".");
        File createDDLFile = null;
        try {
            createDDLFile = new File(createDDLDir, targetTableName + DataXCfgFile.DATAX_CREATE_DDL_FILE_NAME_SUFFIX);
            FileUtils.write(createDDLFile, ddl.getDDLScript(), TisUTF8.get());

            EasyMock.expect(dataXProcessor.getDataxCreateDDLDir(null)).andReturn(createDDLDir);
            DataxWriter.dataxWriterGetter = (dataXName) -> {
                return writer;
            };
            DataxProcessor.processorGetter = (dataXName) -> {
                Assert.assertEquals(testDataXName, dataXName);
                return dataXProcessor;
            };
            EasyMock.replay(dataXProcessor);
            String[] jdbcUrl = new String[1];
            writer.getDataSourceFactory().getDbConfig().vistDbURL(false, (a, b, url) -> {
                jdbcUrl[0] = url;
            });

            if (StringUtils.isEmpty(jdbcUrl[0])) {
                throw new IllegalStateException("jdbcUrl[0] can not be empty");
            }
            //WriterJson wjson = WriterJson.path("dameng_writer_real_dump.json");
            wjson.addCfgSetter((cfg) -> {
                cfg.set("parameter.connection[0].jdbcUrl", jdbcUrl[0]);
                return cfg;
            });
            Assert.assertFalse("isGenerateCreateDDLSwitchOff shall be false", writer.isGenerateCreateDDLSwitchOff());
            WriterTemplate.realExecuteDump(testDataXName, wjson, writer);

            EasyMock.verify(dataXProcessor);
        } finally {
            FileUtils.deleteQuietly(createDDLFile);
        }
    }


    private DataXDaMengWriter getDamengWriter() {
        DaMengDataSourceFactory dsFactory = TestDaMengDataSourceFactory.createDaMengDataSourceFactory();

        DataXDaMengWriter writer = new DataXDaMengWriter() {
            @Override
            public DaMengDataSourceFactory getDataSourceFactory() {
                return dsFactory;
            }

            @Override
            public Class<?> getOwnerClass() {
                return DataXDaMengWriter.class;
            }
        };
        writer.autoCreateTable = AutoCreateTable.dft();
        ;
        return writer;
    }
}
