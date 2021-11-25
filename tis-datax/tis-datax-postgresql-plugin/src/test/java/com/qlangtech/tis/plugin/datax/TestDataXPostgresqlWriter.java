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

import com.google.common.collect.Lists;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.extension.util.PluginExtraProps;
import com.qlangtech.tis.plugin.common.WriterTemplate;
import com.qlangtech.tis.plugin.datax.test.TestSelectedTabs;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.postgresql.PGDataSourceFactory;
import com.qlangtech.tis.trigger.util.JsonUtil;
import com.qlangtech.tis.util.DescriptorsJSON;
import junit.framework.TestCase;

import java.util.List;
import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-08 11:35
 **/
public class TestDataXPostgresqlWriter extends TestCase {
    public void testGetDftTemplate() {
        String dftTemplate = DataXPostgresqlWriter.getDftTemplate();
        assertNotNull("dftTemplate can not be null", dftTemplate);
    }

    public void testPluginExtraPropsLoad() throws Exception {
        Optional<PluginExtraProps> extraProps = PluginExtraProps.load(DataXPostgresqlWriter.class);
        assertTrue(extraProps.isPresent());
    }

    public void testDescriptorsJSONGenerate() {
        DataXPostgresqlWriter esWriter = new DataXPostgresqlWriter();
        DescriptorsJSON descJson = new DescriptorsJSON(esWriter.getDescriptor());

        JsonUtil.assertJSONEqual(DataXPostgresqlReader.class
                , "postgres-datax-writer-descriptor.json"
                , descJson.getDescriptorsJSON(), (m, e, a) -> {
                    assertEquals(m, e, a);
                });
    }

    public void testTemplateGenerate() throws Exception {

        DataXPostgresqlWriter dataXWriter = getDataXPostgresqlWriter();

        dataXWriter.template = DataXPostgresqlWriter.getDftTemplate();

        dataXWriter.dbName = "order2";
        dataXWriter.postSql = "drop table @table";
        dataXWriter.preSql = "drop table @table";
        dataXWriter.batchSize = 967;
        //dataXWriter.session =""

        Optional<IDataxProcessor.TableMap> tableMapper = TestSelectedTabs.createTableMapper();

        WriterTemplate.valiateCfgGenerate("postgres-datax-writer-assert.json", dataXWriter, tableMapper.get());

        dataXWriter.postSql = null;
        dataXWriter.preSql = null;
        dataXWriter.batchSize = null;

        WriterTemplate.valiateCfgGenerate("postgres-datax-writer-assert-without-option.json", dataXWriter, tableMapper.get());
    }

    private DataXPostgresqlWriter getDataXPostgresqlWriter() {
        PGDataSourceFactory ds = TestDataXPostgresqlReader.createDataSource();
        return new DataXPostgresqlWriter() {
            @Override
            public PGDataSourceFactory getDataSourceFactory() {
                return ds;
            }

            @Override
            public Class<?> getOwnerClass() {
                return DataXPostgresqlWriter.class;
            }
        };
    }


    public void testAutoCreateDDL() {
        DataXPostgresqlWriter dataXPostgresqlWriter = getDataXPostgresqlWriter();
        dataXPostgresqlWriter.autoCreateTable = true;
        DataxWriter.BaseDataxWriterDescriptor writerDescriptor = dataXPostgresqlWriter.getWriterDescriptor();
        assertTrue("isSupportTabCreate", writerDescriptor.isSupportTabCreate());


        IDataxProcessor.TableMap tableMapper = createCustomer_order_relationTableMap();

        StringBuffer createDDL = dataXPostgresqlWriter.generateCreateDDL(tableMapper);
        assertNotNull(createDDL);
        // 多主键
        assertEquals(IOUtils.loadResourceFromClasspath(TestDataXPostgresqlWriter.class, "multi-pks-create-ddl.txt"), createDDL.toString());

        Optional<ISelectedTab.ColMeta> firstCustomerregisterId = tableMapper.getSourceCols().stream().filter((col) -> customerregisterId.equals(col.getName())).findFirst();
        assertTrue(firstCustomerregisterId.isPresent());
        firstCustomerregisterId.get().setPk(false);

        createDDL = dataXPostgresqlWriter.generateCreateDDL(tableMapper);
        assertNotNull(createDDL);
        // 单主键
        assertEquals(IOUtils.loadResourceFromClasspath(TestDataXPostgresqlWriter.class, "single-pks-create-ddl.txt"), createDDL.toString());
        //  System.out.println(createDDL.toString());
    }

    private final static String customerregisterId = "customerregister_id";

    private static IDataxProcessor.TableMap createCustomer_order_relationTableMap() {
        ISelectedTab.ColMeta colMeta = null;
        IDataxProcessor.TableMap tableMap = new IDataxProcessor.TableMap();
        tableMap.setTo("customer_order_relation");
        List<ISelectedTab.ColMeta> sourceCols = Lists.newArrayList();

        colMeta = new ISelectedTab.ColMeta();
        colMeta.setName(customerregisterId);
        colMeta.setType(ISelectedTab.DataXReaderColType.STRING.dataType);
        colMeta.setPk(true);
        sourceCols.add(colMeta);

        colMeta = new ISelectedTab.ColMeta();
        colMeta.setName("waitingorder_id");
        colMeta.setType(ISelectedTab.DataXReaderColType.STRING.dataType);
        colMeta.setPk(true);
        sourceCols.add(colMeta);

        colMeta = new ISelectedTab.ColMeta();
        colMeta.setName("kind");
        colMeta.setType(ISelectedTab.DataXReaderColType.INT.dataType);
        sourceCols.add(colMeta);

        colMeta = new ISelectedTab.ColMeta();
        colMeta.setName("create_time");
        colMeta.setType(ISelectedTab.DataXReaderColType.Long.dataType);
        sourceCols.add(colMeta);

        colMeta = new ISelectedTab.ColMeta();
        colMeta.setName("last_ver");
        colMeta.setType(ISelectedTab.DataXReaderColType.INT.dataType);
        sourceCols.add(colMeta);

        tableMap.setSourceCols(sourceCols);
        return tableMap;
    }
}
