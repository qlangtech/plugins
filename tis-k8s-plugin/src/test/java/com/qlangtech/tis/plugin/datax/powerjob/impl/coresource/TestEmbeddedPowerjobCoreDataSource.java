package com.qlangtech.tis.plugin.datax.powerjob.impl.coresource;

import com.qlangtech.tis.plugin.datax.powerjob.K8SDataXJobWorker;
import com.qlangtech.tis.plugin.datax.powerjob.TestK8SDataXJobWorker;
import io.kubernetes.client.openapi.ApiException;
import junit.framework.TestCase;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/12/13
 */
public class TestEmbeddedPowerjobCoreDataSource extends TestCase {
    public static final String K8S_IMAGE = "local-k8s";
  //  public static final String K8S_IMAGE = "aliyun";

    // public static final String K8S_IMAGE = "aliyun-k8s";
    public void testLaunchMetaStoreService() throws Exception {

        EmbeddedPowerjobCoreDataSource coreDataSource = new EmbeddedPowerjobCoreDataSource();

        K8SDataXJobWorker powerJobServer = TestK8SDataXJobWorker.createPowerJobServer(null);// new K8SDataXPowerJobServer();
        // powerJobServer.k8sImage = K8S_IMAGE;

        try {
            coreDataSource.launchMetaStore(powerJobServer);
        } catch (ApiException e) {
            throw new RuntimeException(e.getResponseBody(), e);
        }

    }
}
