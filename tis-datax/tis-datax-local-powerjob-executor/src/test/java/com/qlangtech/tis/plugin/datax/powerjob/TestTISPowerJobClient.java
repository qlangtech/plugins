package com.qlangtech.tis.plugin.datax.powerjob;

import com.qlangtech.tis.datax.job.IRegisterApp;
import com.qlangtech.tis.datax.job.PowerjobOrchestrateException;
import junit.framework.TestCase;
import org.junit.Assert;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/11/27
 */
public class TestTISPowerJobClient extends TestCase {

    public void testFetchAllWorkflow() {
        TISPowerJobClient powerJobClient =  TISPowerJobClient.create("192.168.28.200:7700", "tis", "hello1234");

        Assert.assertNotNull(powerJobClient.fetchAllWorkflow(0, 10));
    }

    public void testRegisterApp() throws PowerjobOrchestrateException {
        IRegisterApp registerApp = TISPowerJobClient.registerApp();

        registerApp.registerApp("192.168.64.3:31111", "tis", "123456");

//        TISPowerJobClient client = new TISPowerJobClient("192.168.64.3:31111", "tis", "1");
//        Assert.assertNotNull(client);
    }
}
