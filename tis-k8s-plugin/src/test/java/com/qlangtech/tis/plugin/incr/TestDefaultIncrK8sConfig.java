package com.qlangtech.tis.plugin.incr;

import junit.framework.TestCase;

/**
 * @author: baisui 百岁
 * @create: 2020-08-17 11:22
 **/
public class TestDefaultIncrK8sConfig extends TestCase {

    public void testDefaultIncrK8sConfig() throws Exception {
        DefaultIncrK8sConfig incrK8sCfg = new DefaultIncrK8sConfig();
        incrK8sCfg.namespace = "tis";
        incrK8sCfg.k8sName = "minikube";
        incrK8sCfg.imagePath = "registry.cn-hangzhou.aliyuncs.com/tis/tis-incr:latest";

        incrK8sCfg.getIncrSync().removeInstance("search4totalpay");
    }
}
