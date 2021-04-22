/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 *   This program is free software: you can use, redistribute, and/or modify
 *   it under the terms of the GNU Affero General Public License, version 3
 *   or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 *  This program is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *   FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.tis.plugin.datax;

import com.alibaba.citrus.turbine.Context;
import com.google.common.collect.Lists;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.datax.IDataxGlobalCfg;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxWriter;
import com.qlangtech.tis.datax.impl.DataXCfgGenerator;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.PluginFormProperties;
import com.qlangtech.tis.extension.util.PluginExtraProps;
import com.qlangtech.tis.plugin.BasicTest;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.DataSourceFactoryPluginStore;
import com.qlangtech.tis.plugin.ds.PostedDSProp;
import com.qlangtech.tis.plugin.ds.mysql.MySQLDataSourceFactory;
import com.qlangtech.tis.util.IPluginContext;
import org.easymock.EasyMock;

import java.util.Collections;
import java.util.Optional;

/**
 * @author: baisui 百岁
 * @create: 2021-04-08 11:31
 **/
public class TestDataxMySQLReader extends BasicTest {

    public static String dbName = "baisuitestdb";

    public void testTempateGenerate() throws Exception {

        Optional<PluginExtraProps> extraProps = PluginExtraProps.load(DataxMySQLReader.class);
        assertTrue("DataxMySQLReader extraProps shall exist", extraProps.isPresent());


        IPluginContext pluginContext = EasyMock.createMock("pluginContext", IPluginContext.class);
        Context context = EasyMock.createMock("context", Context.class);
        EasyMock.expect(context.hasErrors()).andReturn(false);


        DataSourceFactoryPluginStore dbStore = TIS.getDataBasePluginStore(new PostedDSProp(dbName));
        //IPluginContext pluginContext = null;
        MySQLDataSourceFactory mysqlDs = new MySQLDataSourceFactory();

        String userName = "root";
        String password = "123456";
        String tabName = "orderdetail";

        mysqlDs.dbName = dbName;
        mysqlDs.port = 3306;
        mysqlDs.encode = "utf8";
        mysqlDs.userName = userName;
        mysqlDs.password = password;
        mysqlDs.nodeDesc = "192.168.28.200[0-7]";
        Descriptor.ParseDescribable<DataSourceFactory> desc = new Descriptor.ParseDescribable<>(mysqlDs);
        pluginContext.addDb(desc, dbName, context, true);

        IDataxProcessor processor = EasyMock.mock("dataxProcessor", IDataxProcessor.class);
        IDataxWriter dataxWriter = EasyMock.mock("dataxWriter", IDataxWriter.class);
        IDataxGlobalCfg dataxGlobalCfg = EasyMock.mock("dataxGlobalCfg", IDataxGlobalCfg.class);
        // IDataxReaderContext dataxReaderContext = EasyMock.mock("dataxReaderContext", IDataxReaderContext.class);


        MySQLDataXReaderContext dataxReaderContext = new MySQLDataXReaderContext(tabName + "_0", tabName);
        dataxReaderContext.jdbcUrl = TestDataxMySQLWriter.mysqlJdbcUrl;
        dataxReaderContext.tabName = tabName;
        dataxReaderContext.username = userName;
        dataxReaderContext.password = password;
        dataxReaderContext.cols = Lists.newArrayList("col1", "col2", "col3");//tableMetadata.stream().map((t) -> t.getValue()).collect(Collectors.toList());

        EasyMock.expect(processor.getWriter()).andReturn(dataxWriter);
        EasyMock.expect(processor.getDataXGlobalCfg()).andReturn(dataxGlobalCfg);
        EasyMock.replay(pluginContext, context, processor, dataxGlobalCfg);
        assertTrue("save mysql db Config faild", dbStore.setPlugins(pluginContext, Optional.of(context), Collections.singletonList(desc)));


        DataxMySQLReader mySQLReader = new DataxMySQLReader();
        mySQLReader.template = DataxMySQLReader.getDftTemplate();
        Descriptor<DataxReader> descriptor = mySQLReader.getDescriptor();
        assertNotNull(descriptor);

        PluginFormProperties propertyTypes = descriptor.getPluginFormPropertyTypes();
        assertEquals(3, propertyTypes.getKVTuples().size());


        DataXCfgGenerator dataProcessor = new DataXCfgGenerator(processor) {
            @Override
            public String getTemplateContent() {
                return mySQLReader.getTemplate();
            }
        };

        String readerCfg = dataProcessor.generateDataxConfig(dataxReaderContext, Optional.empty());
        assertNotNull(readerCfg);
        System.out.println(readerCfg);
        EasyMock.verify(pluginContext, context, processor, dataxGlobalCfg);
    }

}
