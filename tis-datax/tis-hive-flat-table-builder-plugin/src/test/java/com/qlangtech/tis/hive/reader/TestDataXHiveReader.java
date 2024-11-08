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

package com.qlangtech.tis.hive.reader;

import com.qlangtech.tis.extension.util.PluginExtraProps;
import com.qlangtech.tis.plugin.common.PluginDesc;
import com.qlangtech.tis.plugin.common.ReaderTemplate;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.Collections;
import java.util.Optional;

public class TestDataXHiveReader extends TestCase {

    private static final String dataXName = "testDataXName";

    @Test
    public void testDescGenerate() {
        PluginDesc.testDescGenerate(DataXHiveReader.class, "hive-datax-reader-descriptor.json");
    }

    @Test
    public void testTempateGenerate() throws Exception {

        Optional<PluginExtraProps> extraProps = PluginExtraProps.load(DataXHiveReader.class);
        assertTrue("DataXHiveReader extraProps shall exist", extraProps.isPresent());

//        MySQLDataSourceFactory mysqlDataSource = EasyMock.createMock("mysqlDataSourceFactory", MySQLDataSourceFactory.class);
//        mysqlDataSource.splitTableStrategy = new NoneSplitTableStrategy();
//
//        SplitTableStrategy.SplitableTableInDB tabsInDB
//                = new SplitTableStrategy.SplitableTableInDB(mysqlDataSource, SplitTableStrategy.PATTERN_PHYSICS_TABLE);
//        tabsInDB.add(TestDataxMySQLWriter.mysqlJdbcUrl, TestSelectedTabs.tabNameOrderDetail + "_01");
//        tabsInDB.add(TestDataxMySQLWriter.mysqlJdbcUrl, TestSelectedTabs.tabNameOrderDetail + "_02");
//
//        EasyMock.expect(mysqlDataSource.getTablesInDB()).andReturn(tabsInDB).times(1);
//
//        EasyMock.expect(mysqlDataSource.getPassword()).andReturn(password).anyTimes();
//        EasyMock.expect(mysqlDataSource.getUserName()).andReturn(userName).anyTimes();
//        IDataSourceDumper dataDumper = EasyMock.createMock(TestSelectedTabs.tabNameOrderDetail + "TableDumper", IDataSourceDumper.class);
//        EasyMock.expect(dataDumper.getDbHost()).andReturn(TestDataxMySQLWriter.mysqlJdbcUrl).anyTimes();
//// int index, String key, int type, boolean pk
//        TISTable targetTable = new TISTable();
//        targetTable.setTableName(TestSelectedTabs.tabNameOrderDetail);
//
//        EasyMock.expect(mysqlDataSource.getTableMetadata(false, EntityName.parse(TestSelectedTabs.tabNameOrderDetail)))
//                .andReturn(TestSelectedTabs.tabColsMetaOrderDetail).anyTimes();
//
//        EasyMock.expect(mysqlDataSource.getDataDumpers(targetTable)).andDelegateTo(new MySQLDataSourceFactory() {
//            @Override
//            public DataDumpers getDataDumpers(TISTable table) {
//                return new DataDumpers(1, Collections.singletonList(dataDumper).iterator());
//            }
//        }).anyTimes();
//
//        EasyMock.replay(mysqlDataSource, dataDumper);


        DataXHiveReader mySQLReader = new DataXHiveReader() {
            @Override
            public Class<?> getOwnerClass() {
                return DataXHiveReader.class;
            }
        };
        mySQLReader.dataXName = dataXName;
        mySQLReader.template = DataXHiveReader.getDftTemplate();
        //mySQLReader.

        SelectedTab selectedTab = TestDataXHiveReaderRealDump.parseTab();
//        selectedTab.setCols(Lists.newArrayList("col2", "col1", "col3"));
//        selectedTab.setWhere("delete = 0");
//        selectedTab.name = TestSelectedTabs.tabNameOrderDetail;
        mySQLReader.selectedTabs = (Collections.singletonList(selectedTab));
        //校验证列和 where条件都设置的情况
        // valiateReaderCfgGenerate("mysql-datax-reader-assert.json", processor, mySQLReader);

        ReaderTemplate.validateDataXReader("hive-datax-reader-assert.json", dataXName, mySQLReader);

//        mySQLReader.setSelectedTabs(TestSelectedTabs.createSelectedTabs(1));
//
//        ReaderTemplate.validateDataXReader("mysql-datax-reader-asser-without-option-val.json", dataXName, mySQLReader);
//
//        // DefaultSplitTableStrategy splitTableStrategy = SplitTableStrategyUtils.createSplitTableStrategy();
//        mysqlDataSource.splitTableStrategy = SplitTableStrategyUtils.createSplitTableStrategy();
//        ReaderTemplate.validateDataXReader("mysql-datax-reader-assert-split-tabs.json", dataXName, mySQLReader);
//
//        EasyMock.verify(mysqlDataSource, dataDumper);
    }
}
