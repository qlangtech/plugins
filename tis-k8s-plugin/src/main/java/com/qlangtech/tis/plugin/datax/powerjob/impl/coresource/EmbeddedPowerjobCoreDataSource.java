package com.qlangtech.tis.plugin.datax.powerjob.impl.coresource;

import com.google.common.collect.Lists;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.config.k8s.ReplicasSpec;
import com.qlangtech.tis.config.k8s.impl.DefaultK8SImage;
import com.qlangtech.tis.coredefine.module.action.impl.RcDeployment;
import com.qlangtech.tis.datax.TimeFormat;
import com.qlangtech.tis.datax.job.IRegisterApp;
import com.qlangtech.tis.datax.job.PowerjobOrchestrateException;
import com.qlangtech.tis.datax.job.SSERunnable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.ExtensionList;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.datax.powerjob.K8SDataXPowerJobServer;
import com.qlangtech.tis.plugin.datax.powerjob.PowerJobK8SImage;
import com.qlangtech.tis.plugin.datax.powerjob.PowerjobCoreDataSource;
import com.qlangtech.tis.plugin.k8s.K8SController;
import com.qlangtech.tis.plugin.k8s.K8SUtils;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1ContainerPort;
import io.kubernetes.client.openapi.models.V1EnvVar;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;

import static com.qlangtech.tis.plugin.datax.powerjob.K8SDataXPowerJobServer.K8S_DATAX_POWERJOB_MYSQL;

/**
 * 使用Powerjob 默认的
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/10/31
 */
public class EmbeddedPowerjobCoreDataSource extends PowerjobCoreDataSource {
    private static final int mysqlPort3306 = 3306;
    private static final String mysql3306 = "mysqlport";
    private static final Logger logger = LoggerFactory.getLogger(EmbeddedPowerjobCoreDataSource.class);

    //    @Override
//    public String createCoreJdbcParams() {
//    }
    private IRegisterApp getTISPowerjobClient() {

        ExtensionList<IRegisterApp> extensionList = TIS.get().getExtensionList(IRegisterApp.class);
        for (IRegisterApp resisterApp : extensionList) {
            return resisterApp;
        }

        throw new IllegalStateException("can not find instanceof " + IRegisterApp.class.getSimpleName());
//        return (ITISPowerJob) DataXJobWorker.getJobWorker( //
//                DataXJobWorker.K8S_DATAX_INSTANCE_NAME, Optional.of(DataXJobWorker.K8SWorkerCptType.UsingExistCluster));
    }

    @Override
    public void initialPowerjobAccount(K8SDataXPowerJobServer powerjobServer) {
        IRegisterApp tisPowerJob = getTISPowerjobClient();
        SSERunnable sse = SSERunnable.getLocal();
        // String powerjobDomain, String appName, String password
        final String linkHost = powerjobServer.serverPortExport.getPowerjobHost();
//        try {
//            // 检查账户是否存在
//
//            // 存在则直接跳过
//
//            // 不存在？则创建
//            (tisPowerJob) //
//                    .registerApp(linkHost, powerjobServer.appName, powerjobServer.password);
//        } catch (Exception e) {
//            throw new IllegalStateException("server:" + linkHost + ",appname:" + powerjobServer.appName, e);
//        }

        int tryCount = 0;
        final int tryLimit = 100;
        while (true) {
            try {
                //this.serverPortExport.initialPowerjobAccount(this);
                // this.coreDS.initialPowerjobAccount(this);

                (tisPowerJob) //
                        .registerApp(linkHost, powerjobServer.appName, powerjobServer.password);

                // 需要打印日志
                logger.info("success initialPowerjobAccount");
                sse.info(K8SDataXPowerJobServer.K8S_DATAX_POWERJOB_REGISTER_ACCOUNT.getName(), TimeFormat.getCurrentTimeStamp()
                        , "success initialPowerjobAccount with appName:" + powerjobServer.appName);
                break;
            } catch (Exception e) {
                int idxOfError = -1;
                if (tryCount++ < tryLimit && ( //
                        (idxOfError = ExceptionUtils.indexOfThrowable(e, java.net.SocketTimeoutException.class)) > -1 ||
                                (idxOfError = ExceptionUtils.indexOfThrowable(e, java.net.ConnectException.class)) > -1)
                ) {

                    // 说明是超时等待片刻即可
                    logger.warn("contain " + ExceptionUtils.getThrowableList(e).get(idxOfError).getMessage()
                            + " tryCount:" + tryCount + " " + e.getMessage());

                    try {
                        Thread.sleep(4500);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                } else {
                    // 其他日志直接中断了
                    throw new RuntimeException(e);
                }
            }
        }

    }

    @Override
    protected String getJdbcUrl() {
        return "jdbc:mysql://" + K8SDataXPowerJobServer.K8S_DATAX_POWERJOB_MYSQL_SERVICE.getHostPortReplacement() + "/powerjob-daily?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai";
    }

    @Override
    public RcDeployment getRCDeployment(K8SController k8SController) {
        return k8SController.getRCDeployment(K8SDataXPowerJobServer.K8S_DATAX_POWERJOB_MYSQL);
    }

    @Override
    public void launchMetaStore(K8SDataXPowerJobServer powerJobServer) throws ApiException, PowerjobOrchestrateException {
        SSERunnable sse = SSERunnable.getLocal();
        // 需要启动一个pod的mysql实例
        //  boolean success = false;

        ReplicasSpec mysqlRcSpec = powerJobServer.getReplicasSpec();// K8SUtils.createDftReplicasSpec();
        PowerJobK8SImage pjImage = powerJobServer.getImage();
        DefaultK8SImage powerjobMySQLImage = new DefaultK8SImage();
        powerjobMySQLImage.imagePath = pjImage.embeddedMetaDataImagePath;// "powerjob/powerjob-mysql:latest";
        powerjobMySQLImage.namespace = pjImage.getNamespace();// powerJobServer.getPowerJobImage().getNamespace();


        CoreV1Api v1Api;
        try {
            List<V1ContainerPort> exportPorts = Lists.newArrayList();
            V1ContainerPort mysqlPort = new V1ContainerPort();
            mysqlPort.setContainerPort(mysqlPort3306);
            // mysqlPort.setHostPort(3306);
            mysqlPort.setName(mysql3306);
            mysqlPort.setProtocol("TCP");
            exportPorts.add(mysqlPort);

            List<V1EnvVar> mysqlEnvs = Lists.newArrayList();
            V1EnvVar var = new V1EnvVar();
            var.setName("MYSQL_ROOT_HOST");
            var.setValue("%");
            mysqlEnvs.add(var);

            var = new V1EnvVar();
            var.setName("MYSQL_ROOT_PASSWORD");
            var.setValue("No1Bug2Please3!");
            mysqlEnvs.add(var);
            v1Api = new CoreV1Api(powerJobServer.getK8SApi());
            final String reVersion = K8SUtils.getResourceVersion(K8SUtils.createReplicationController(
                    v1Api, powerjobMySQLImage, K8S_DATAX_POWERJOB_MYSQL, () -> {
                        V1Container container = new V1Container();
                        container.setArgs(Collections.singletonList("--lower_case_table_names=1"));
                        return container;
                    }, mysqlRcSpec, exportPorts, mysqlEnvs));

            // String namespace, V1Service body, String pretty, String dryRun, String fieldManager

            K8SUtils.waitReplicaControllerLaunch(powerjobMySQLImage //
                    , K8S_DATAX_POWERJOB_MYSQL, mysqlRcSpec, powerJobServer.getK8SApi(), reVersion);
            // success = true;
        } finally {
            // sse.writeComplete(K8SDataXPowerJobServer.K8S_DATAX_POWERJOB_MYSQL, success);
        }


    }

    @Override
    public void launchMetaStoreService(K8SDataXPowerJobServer powerJobServer) throws ApiException {
        K8SUtils.createService(new CoreV1Api(powerJobServer.getK8SApi()), powerJobServer.getImage().namespace //
                , K8SDataXPowerJobServer.K8S_DATAX_POWERJOB_MYSQL_SERVICE, K8S_DATAX_POWERJOB_MYSQL, mysqlPort3306, mysql3306);
    }

    @TISExtension
    public static class DefaultDesc extends Descriptor<PowerjobCoreDataSource> {
        @Override
        public String getDisplayName() {
            return "Embedded";
        }
    }
}
