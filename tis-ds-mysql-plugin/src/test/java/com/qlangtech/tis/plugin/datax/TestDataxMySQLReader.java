package com.qlangtech.tis.plugin.datax;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.PluginFormProperties;
import com.qlangtech.tis.extension.util.PluginExtraProps;
import com.qlangtech.tis.manage.common.CenterResource;
import com.qlangtech.tis.manage.common.HttpUtils;
import com.qlangtech.tis.plugin.BasicTest;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.DataSourceFactoryPluginStore;
import com.qlangtech.tis.plugin.ds.PostedDSProp;
import com.qlangtech.tis.plugin.ds.mysql.MySQLDataSourceFactory;
import com.qlangtech.tis.util.IPluginContext;
import junit.framework.TestCase;
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



        //List<DataSourceFactory> allDbs = DataSourceFactory.all();
        //assertTrue("allDbs.size():" + allDbs.size(), allDbs.size() > 0);


        IPluginContext pluginContext = EasyMock.createMock("pluginContext", IPluginContext.class);
        Context context = EasyMock.createMock("context", Context.class);
        EasyMock.expect(context.hasErrors()).andReturn(false);


        DataSourceFactoryPluginStore dbStore = TIS.getDataBasePluginStore(new PostedDSProp(dbName));
        //IPluginContext pluginContext = null;
        MySQLDataSourceFactory mysqlDs = new MySQLDataSourceFactory();

        mysqlDs.dbName = dbName;
        mysqlDs.port = 3306;
        mysqlDs.encode = "utf8";
        mysqlDs.userName = "root";
        mysqlDs.password = "123456";
        mysqlDs.nodeDesc = "192.168.28.200[0-7]";
        Descriptor.ParseDescribable<DataSourceFactory> desc = new Descriptor.ParseDescribable<>(mysqlDs);
        pluginContext.addDb(desc, dbName, context, true);
        EasyMock.replay(pluginContext, context);
        assertTrue("save mysql db Config faild", dbStore.setPlugins(pluginContext, Optional.of(context), Collections.singletonList(desc)));


        EasyMock.verify(pluginContext, context);
        DataxMySQLReader mySQLReader = new DataxMySQLReader();
        Descriptor<DataxReader> descriptor = mySQLReader.getDescriptor();
        assertNotNull(descriptor);

        PluginFormProperties propertyTypes = descriptor.getPluginFormPropertyTypes();
        assertEquals(3, propertyTypes.getKVTuples().size());
    }

}
