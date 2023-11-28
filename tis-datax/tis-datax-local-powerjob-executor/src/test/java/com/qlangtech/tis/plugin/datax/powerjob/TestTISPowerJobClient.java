package com.qlangtech.tis.plugin.datax.powerjob;

import junit.framework.TestCase;
import org.junit.Assert;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/11/27
 */
public class TestTISPowerJobClient extends TestCase {

    public void testFetchAllWorkflow() {
        TISPowerJobClient powerJobClient = new TISPowerJobClient("192.168.28.200:7700", "tis", "hello1234");

        Assert.assertNotNull(powerJobClient.fetchAllWorkflow(0, 10));
    }
}
