package com.qlangtech.tis.plugin.ds.oracle;

import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import org.junit.Assert;
import org.junit.Test;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/10/9
 */
public class TestKingBaseDSFactoryContainer {

    @Test
    public void testGetConnection() {
        DataSourceFactory kingBaseFactory = KingBaseDSFactoryContainer.initialize((conn) -> {
        });
        kingBaseFactory.visitFirstConnection((conn) -> {

            Assert.assertNotNull(conn);

        });
    }
}
