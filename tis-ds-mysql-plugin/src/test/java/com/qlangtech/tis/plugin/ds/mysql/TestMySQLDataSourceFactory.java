package com.qlangtech.tis.plugin.ds.mysql;

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.manage.common.CenterResource;
import com.qlangtech.tis.manage.common.HttpUtils;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.DataSourceFactoryPluginStore;
import com.qlangtech.tis.plugin.ds.FacadeDataSource;
import com.qlangtech.tis.plugin.ds.PostedDSProp;
import junit.framework.TestCase;

import javax.sql.DataSource;

/**
 * @author: baisui 百岁
 * @create: 2020-11-24 17:42
 **/
public class TestMySQLDataSourceFactory extends TestCase {

    private static final String DB_ORDER = "order1";

    static {
        CenterResource.setNotFetchFromCenterRepository();
        HttpUtils.addMockGlobalParametersConfig();
    }

    public void testGetPlugin() {

        DataSourceFactoryPluginStore dbPluginStore = TIS.getDataBasePluginStore(null, new PostedDSProp(DB_ORDER));

        DataSourceFactory dataSourceFactory = dbPluginStore.getPlugin();

        assertNotNull(dataSourceFactory);

        FacadeDataSource datasource = dbPluginStore.createFacadeDataSource();
        assertNotNull(datasource);

//        List<Descriptor<DataSourceFactory>> descList
//                = TIS.get().getDescriptorList(DataSourceFactory.class);
//        assertNotNull(descList);
//        assertEquals(1, descList.size());


//        Descriptor<DataSourceFactory> mysqlDS = descList.get(0);
//
//        mysqlDS.validate()
    }
}
