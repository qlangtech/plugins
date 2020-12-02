package com.qlangtech.tis.plugin.ds.mysql;

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import junit.framework.TestCase;

import java.util.List;

/**
 * @author: baisui 百岁
 * @create: 2020-11-24 17:42
 **/
public class TestMySQLDataSourceFactory extends TestCase {

    public void testGetPlugin() {
        List<Descriptor<DataSourceFactory>> descList
                = TIS.get().getDescriptorList(DataSourceFactory.class);
        assertNotNull(descList);
        assertEquals(1, descList.size());


//        Descriptor<DataSourceFactory> mysqlDS = descList.get(0);
//
//        mysqlDS.validate()
    }
}
