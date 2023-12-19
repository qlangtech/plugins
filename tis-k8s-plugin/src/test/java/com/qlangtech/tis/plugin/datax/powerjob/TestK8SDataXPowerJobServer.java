package com.qlangtech.tis.plugin.datax.powerjob;

import com.qlangtech.tis.config.k8s.ReplicasSpec;
import com.qlangtech.tis.plugin.datax.powerjob.impl.coresource.EmbeddedPowerjobCoreDataSource;
import com.qlangtech.tis.plugin.datax.powerjob.impl.coresource.TestEmbeddedPowerjobCoreDataSource;
import com.qlangtech.tis.plugin.datax.powerjob.impl.serverport.NodePort;
import com.qlangtech.tis.plugin.k8s.K8SUtils;
import io.kubernetes.client.openapi.ApiException;
import junit.framework.TestCase;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/12/14
 */
public class TestK8SDataXPowerJobServer extends TestCase {

    /**
     * 启动worker
     *
     * @throws Exception
     */
    public void testLaunchPowerjobWorker() throws Exception {

        try {
            final K8SDataXPowerJobWorker powerJobWorker = new K8SDataXPowerJobWorker();
            powerJobWorker.port = 27777;
           // powerJobWorker.appName = "tis";
            powerJobWorker.storeStrategy = "MEMORY";
            powerJobWorker.maxResultLength = 8096;
            powerJobWorker.maxLightweightTaskNum = 1024;
            powerJobWorker.maxHeavyweightTaskNum = 64;
            powerJobWorker.healthReportInterval = 10;
            powerJobWorker.k8sImage = TestEmbeddedPowerjobCoreDataSource.K8S_IMAGE;


            ReplicasSpec replicasSpec = K8SUtils.createDftReplicasSpec();
            powerJobWorker.setReplicasSpec(replicasSpec);
            K8SDataXPowerJobServer powerJobServer = new K8SDataXPowerJobServer() {
                @Override
                protected K8SDataXPowerJobWorker getPowerJobWorker() {
                    return powerJobWorker;
                }
            };
            powerJobServer.k8sImage =
                    TestEmbeddedPowerjobCoreDataSource.K8S_IMAGE;

            powerJobServer.launchPowerjobWorker();
        } catch (ApiException e) {
            throw new RuntimeException(e.getResponseBody());
        }
    }

    public void testLaunchPowerjobServer() throws Exception {
        try {
            K8SDataXPowerJobServer powerJobServer = new K8SDataXPowerJobServer();
            EmbeddedPowerjobCoreDataSource coreDataSource = new EmbeddedPowerjobCoreDataSource();

            NodePort portExport = new NodePort();
            portExport.serverPort = 7700;
            portExport.nodePort = 31111;

            powerJobServer.serverPortExport = portExport;// 7700;

            PowerJobOMS oms = new PowerJobOMS();
            oms.akkaPort = 10086;
            oms.httpPort = 10010;
            powerJobServer.omsProfile = oms;
            powerJobServer.coreDS = coreDataSource;
            powerJobServer.k8sImage =
                    TestEmbeddedPowerjobCoreDataSource.K8S_IMAGE;

            powerJobServer.setReplicasSpec(K8SUtils.createDftReplicasSpec());

            powerJobServer.launchPowerjobServer();
        } catch (ApiException e) {
            throw new RuntimeException(e.getResponseBody());
        }
    }
}
