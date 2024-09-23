package com.qlangtech.tis.plugin.datax.powerjob;

import com.google.common.collect.Lists;
import com.google.gson.reflect.TypeToken;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.config.k8s.ReplicasSpec;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.job.DataXJobWorker;
import com.qlangtech.tis.datax.job.SSERunnable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.fullbuild.indexbuild.RunningStatus;
import com.qlangtech.tis.plugin.datax.powerjob.impl.PowerJobPodLogListener;
import com.qlangtech.tis.plugin.datax.powerjob.impl.coresource.EmbeddedPowerjobCoreDataSource;
import com.qlangtech.tis.plugin.datax.powerjob.impl.coresource.TestEmbeddedPowerjobCoreDataSource;
import com.qlangtech.tis.plugin.datax.powerjob.impl.serverport.LoadBalance;
import com.qlangtech.tis.plugin.datax.powerjob.impl.serverport.NodePort;
import com.qlangtech.tis.plugin.incr.WatchPodLog;
import com.qlangtech.tis.plugin.k8s.K8SUtils;
import com.qlangtech.tis.plugin.k8s.K8SUtils.PodStat;
import com.qlangtech.tis.plugin.k8s.K8SUtils.WaitReplicaControllerLaunch;
import com.qlangtech.tis.plugin.k8s.NamespacedEventCallCriteria;
import com.qlangtech.tis.trigger.socket.ExecuteState;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.apis.CoreV1Api.APIlistNamespacedPodRequest;
import io.kubernetes.client.openapi.models.V1Pod;
import io.kubernetes.client.util.Watch;
import io.kubernetes.client.util.Watch.Response;
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
        SSERunnable.setLocalThread(SSERunnable.createMock());
    }

    public void testScalePodNumber() throws Exception {
        K8SDataXPowerJobServer powerJobServer = createPowerJobServer(null);
        SSERunnable sse = SSERunnable.getLocal();
        TargetResName cptType = K8SDataXPowerJobServer.K8S_DATAX_POWERJOB_WORKER;
        powerJobServer.updatePodNumber(sse, cptType, 2);
    }

    public void testWaitPowerjobPods() throws Exception {
        K8SDataXPowerJobServer powerJobServer = createPowerJobServer(null);
        PowerJobK8SImage image = powerJobServer.getImage();

        CoreV1Api apiClient = powerJobServer.getK8SApi();

        APIlistNamespacedPodRequest apIlistNamespacedPodRequest = apiClient.listNamespacedPod(image.getNamespace());
        String resourceVersion = apIlistNamespacedPodRequest.execute().getMetadata().getResourceVersion();
        System.out.println("resourceVersion:" + resourceVersion);


//        apIlistNamespacedPodRequest = apiClient.listNamespacedPod(image.getNamespace());
//        V1PodList pods = apIlistNamespacedPodRequest
//                .resourceVersion(resourceVersion)
//                .watch(true)
//                .execute();
//        System.out.println("resver:" + pods.getMetadata().getResourceVersion() + "," + pods.getItems().size());


//        try (Watch<V1Pod> rcWatch = Watch.createWatch(apiClient.getApiClient()
//                //
//                ,
//                apiClient.listNamespacedPod(image.getNamespace())
//                       // .allowWatchBookmarks(false)
//                        .watch(true)
//                        .resourceVersion(resourceVersion)
//                        .buildCall(K8SUtils.createApiCallback())
//
//                //
//                , new TypeToken<Response<V1Pod>>() {
//                }.getType())) {
//            while (rcWatch.hasNext()) {
//                System.out.println("next:" + rcWatch.next().object);
//
//            }
//        }


        // 2547217  "2537336"
        NamespacedEventCallCriteria resVersion
                = NamespacedEventCallCriteria.createResVersion(
                "b398cf1f-b5bb-4452-8dc7-7f67bda0fd2a", "powerjob-server", resourceVersion);

        ReplicasSpec replicasSpec = ReplicasSpec.createDftReplicasSpec();
        WaitReplicaControllerLaunch controllerLaunch = powerJobServer.waitPowerjobPods(image, resVersion, replicasSpec);

        Assert.assertTrue(controllerLaunch.getRelevantPods().size() > 0);
    }

    public void testWatchOneOfPowerJobPodLog() throws Exception {
        K8SDataXPowerJobServer powerJobServer = createPowerJobServer(null);

        WatchPodLog podLog = powerJobServer.watchOneOfPowerJobPodLog(
                new WaitReplicaControllerLaunch(null, Lists.newArrayList(new PodStat(null, RunningStatus.SUCCESS)))
                //        Sets.newHashSet("datax-worker-powerjob-server-pqlml"))
                , new PowerJobPodLogListener() {
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
        } catch (Exception e) {
           throw new RuntimeException(e);
        }
    }

    public void testLaunchPowerjobServer() throws Exception {
        try {

            //  NodePort portExport = createNodePort();

            LoadBalance portExport = createLoadBalance();

            K8SDataXPowerJobServer powerJobServer = createPowerJobServer(portExport);

            powerJobServer.setReplicasSpec(ReplicasSpec.createDftReplicasSpec());

            powerJobServer.launchPowerjobServer();
        } catch (ApiException e) {
            throw new RuntimeException(e.getResponseBody());
        }
    }

    private LoadBalance createLoadBalance() {
        LoadBalance loadBalance = new LoadBalance();
        loadBalance.serverPort = 7700;
       // loadBalance.usingClusterIP = false;
        return loadBalance;
    }

    public void testRegisterApp() throws Exception {
        NodePort portExport = createNodePort();

        K8SDataXPowerJobServer powerJobServer = createPowerJobServer(portExport);
        powerJobServer.coreDS.initialPowerjobAccount(powerJobServer);
        //  portExport.initialPowerjobAccount(powerJobServer);
    }

    private NodePort createNodePort() {
        NodePort portExport = new NodePort();
        portExport.host = "192.168.64.3";
        portExport.serverPort = 7700;
        portExport.nodePort = 31111;
        return portExport;
    }

    public static K8SDataXPowerJobServer createPowerJobServer(ServerPortExport portExport) {

        final K8SDataXPowerJobWorker powerJobWorker = new K8SDataXPowerJobWorker();
        powerJobWorker.port = 27777;
        // powerJobWorker.appName = "tis";
        powerJobWorker.storeStrategy = "MEMORY";
        powerJobWorker.maxResultLength = 8096;
        powerJobWorker.maxLightweightTaskNum = 1024;
        powerJobWorker.maxHeavyweightTaskNum = 64;
        powerJobWorker.healthReportInterval = 10;
        powerJobWorker.k8sImage = TestEmbeddedPowerjobCoreDataSource.K8S_IMAGE;


        ReplicasSpec replicasSpec = ReplicasSpec.createDftReplicasSpec();
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

        powerJobServer.setReplicasSpec(ReplicasSpec.createDftPowerjobServerReplicasSpec());
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
