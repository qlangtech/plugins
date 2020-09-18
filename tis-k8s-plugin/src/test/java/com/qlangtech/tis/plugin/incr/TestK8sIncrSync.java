package com.qlangtech.tis.plugin.incr;

import com.qlangtech.tis.coredefine.module.action.IIncrSync;
import com.qlangtech.tis.coredefine.module.action.IncrDeployment;
import com.qlangtech.tis.manage.common.Config;
import junit.framework.TestCase;

/**
 * @author: baisui 百岁
 * @create: 2020-09-02 15:06
 **/
public class TestK8sIncrSync extends TestCase {


    public void testGetRCDeployment() {
        IIncrSync incrSync = TestDefaultIncrK8sConfig.getIncrSync();
        IncrDeployment rcDeployment = incrSync.getRCDeployment(TestDefaultIncrK8sConfig.totalpay);
        assertNotNull(rcDeployment);
    }


    public void testDeleteDeployment() throws Exception {
        IIncrSync incrSync = TestDefaultIncrK8sConfig.getIncrSync();

        incrSync.removeInstance(Config.S4TOTALPAY);
    }

    public void testLaunchIncrProcess() throws Exception {
        IIncrSync incrSync = TestDefaultIncrK8sConfig.getIncrSync();

        incrSync.relaunch(Config.S4TOTALPAY);

    }
}
