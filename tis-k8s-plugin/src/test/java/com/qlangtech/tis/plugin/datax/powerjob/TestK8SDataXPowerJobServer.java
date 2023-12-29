package com.qlangtech.tis.plugin.datax.powerjob;

import com.google.common.collect.Sets;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.config.k8s.ReplicasSpec;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.job.DataXJobWorker;
import com.qlangtech.tis.datax.job.SSERunnable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.plugin.datax.powerjob.impl.PowerJobPodLogListener;
import com.qlangtech.tis.plugin.datax.powerjob.impl.coresource.EmbeddedPowerjobCoreDataSource;
import com.qlangtech.tis.plugin.datax.powerjob.impl.coresource.TestEmbeddedPowerjobCoreDataSource;
import com.qlangtech.tis.plugin.datax.powerjob.impl.serverport.NodePort;
import com.qlangtech.tis.plugin.incr.WatchPodLog;
import com.qlangtech.tis.plugin.k8s.K8SUtils;
import com.qlangtech.tis.trigger.socket.ExecuteState;
import io.kubernetes.client.openapi.ApiException;
import junit.framework.TestCase;
import org.junit.Assert;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/12/14
 */
public class TestK8SDataXPowerJobServer extends TestCase {

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        SSERunnable.setLocalThread(new SSERunnable() {
            @Override
            public void writeComplete(TargetResName subJob, boolean success) {
            }

            @Override
            public void info(String serviceName, long timestamp, String msg) {
            }

            @Override
            public void error(String serviceName, long timestamp, String msg) {
            }

            @Override
            public void fatal(String serviceName, long timestamp, String msg) {
            }

            @Override
            public void run() {
            }
        });
    }

    public void testScalePodNumber() throws Exception {
        K8SDataXPowerJobServer powerJobServer = createPowerJobServer(null);
        powerJobServer.updatePodNumber(2);
    }

    public void testWatchOneOfPowerJobPodLog() throws Exception {
        K8SDataXPowerJobServer powerJobServer = createPowerJobServer(null);

        WatchPodLog podLog = powerJobServer.watchOneOfPowerJobPodLog(Sets.newHashSet("datax-worker-powerjob-server-pqlml"), new PowerJobPodLogListener() {
            @Override
            protected void consumePodLogMsg(ExecuteState<String> log) {
                System.out.println(log.getMsg());
            }
        });

        Thread.sleep(99000);
        podLog.close();
    }

    public void testDeleteAllResources() {
        K8SDataXPowerJobServer powerJobServer = createPowerJobServer(null);

        Descriptor<DataXJobWorker> descriptor = powerJobServer.getDescriptor();
        Assert.assertNotNull("descriptor can not be null", descriptor);

        powerJobServer.remove();
    }

    /**
     * 启动worker
     *
     * @throws Exception
     */
    public void testLaunchPowerjobWorker() throws Exception {

        try {

            K8SDataXPowerJobServer powerJobServer = createPowerJobServer(null);
            powerJobServer.k8sImage =
                    TestEmbeddedPowerjobCoreDataSource.K8S_IMAGE;

            powerJobServer.launchPowerjobWorker();
        } catch (ApiException e) {
            throw new RuntimeException(e.getResponseBody());
        }
    }

    public void testLaunchPowerjobServer() throws Exception {
        try {

            NodePort portExport = createNodePort();

            K8SDataXPowerJobServer powerJobServer = createPowerJobServer(portExport);

            powerJobServer.setReplicasSpec(K8SUtils.createDftReplicasSpec());

            powerJobServer.launchPowerjobServer();
        } catch (ApiException e) {
            throw new RuntimeException(e.getResponseBody());
        }
    }

    public void testRegisterApp() {
        NodePort portExport = createNodePort();

        K8SDataXPowerJobServer powerJobServer = createPowerJobServer(portExport);

        portExport.initialPowerjobAccount(powerJobServer);
    }

    private NodePort createNodePort() {
        NodePort portExport = new NodePort();
        portExport.host = "192.168.64.3";
        portExport.serverPort = 7700;
        portExport.nodePort = 31111;
        return portExport;
    }

    public static K8SDataXPowerJobServer createPowerJobServer(NodePort portExport) {

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

            @Override
            public final Descriptor<DataXJobWorker> getDescriptor() {
                return TIS.get().getDescriptor(K8SDataXPowerJobServer.class);
            }
        };
        EmbeddedPowerjobCoreDataSource coreDataSource = new EmbeddedPowerjobCoreDataSource();

        powerJobServer.setReplicasSpec(K8SUtils.createDftPowerjobServerReplicasSpec());
        powerJobServer.serverPortExport = portExport;// 7700;

        PowerJobOMS oms = new PowerJobOMS();
        oms.akkaPort = 10086;
        oms.httpPort = 10010;
        powerJobServer.omsProfile = oms;
        powerJobServer.coreDS = coreDataSource;
        powerJobServer.k8sImage =
                TestEmbeddedPowerjobCoreDataSource.K8S_IMAGE;

        powerJobServer.appName = "tis";
        powerJobServer.password = "123456";
        return powerJobServer;
    }


}