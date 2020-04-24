package com.qlangtech.tis.fullbuild.indexbuild.impl;

import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.config.yarn.IYarnConfig;
import com.qlangtech.tis.manage.common.HttpUtils;
import junit.framework.TestCase;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

/**
 * @author: baisui 百岁
 * @create: 2020-04-22 18:50
 **/
public class TestYarnTableDumpFactory extends TestCase {
    public void testGetManagerResourceAddress() throws Exception {
        HttpUtils.addMockGlobalParametersConfig();
        IYarnConfig yarn1 = ParamsConfig.getItem("yarn1", IYarnConfig.class);

       // YarnConfiguration config = Hadoop020RemoteJobTriggerFactory.getYarnConfig(yarn1);

//        System.out.println(config.get("yarn.resourcemanager.scheduler.class"));
//
//        String hostname = config.get("yarn.resourcemanagerr.hostname");

      //  System.out.println(hostname);
    }
}
