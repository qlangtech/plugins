package com.qlangtech.tis.plugin.datax.powerjob.impl.coresource;

import com.google.common.collect.Lists;
import com.qlangtech.tis.config.k8s.ReplicasSpec;
import com.qlangtech.tis.config.k8s.impl.DefaultK8SImage;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.datax.powerjob.K8SDataXPowerJobServer;
import com.qlangtech.tis.plugin.datax.powerjob.PowerjobCoreDataSource;
import com.qlangtech.tis.plugin.k8s.K8SUtils;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Container;
import io.kubernetes.client.openapi.models.V1ContainerPort;
import io.kubernetes.client.openapi.models.V1EnvVar;

import java.util.Collections;
import java.util.List;

import static com.qlangtech.tis.plugin.k8s.K8SUtils.K8S_DATAX_POWERJOB_MYSQL;

/**
 * 使用Powerjob 默认的
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/10/31
 */
public class EmbeddedPowerjobCoreDataSource extends PowerjobCoreDataSource {



    @Override
    public String createCoreJdbcUrl() {
        return "jdbc:mysql://" + K8S_DATAX_POWERJOB_MYSQL_SERVICE.getHostPortReplacement() + "/powerjob-daily?useUnicode=true&characterEncoding=UTF-8&serverTimezone=Asia/Shanghai";
    }

    @Override
    public void launchMetaStoreService(K8SDataXPowerJobServer powerJobServer) throws ApiException {
        // 需要启动一个pod的mysql实例
        ReplicasSpec mysqlRcSpec = K8SUtils.createDftReplicasSpec();

        DefaultK8SImage powerjobMySQLImage = new DefaultK8SImage();
        powerjobMySQLImage.imagePath = "powerjob/powerjob-mysql:latest";
        powerjobMySQLImage.namespace = powerJobServer.getPowerJobImage().getNamespace();


        int mysqlPort3306 = 3306;
        String mysql3306 = "mysqlport";
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
        CoreV1Api v1Api = new CoreV1Api(powerJobServer.getK8SApi());
        K8SUtils.createReplicationController(
                v1Api, powerjobMySQLImage, K8S_DATAX_POWERJOB_MYSQL, () -> {
                    V1Container container = new V1Container();
                    container.setArgs(Collections.singletonList("--lower_case_table_names=1"));
                    return container;
                }, mysqlRcSpec, exportPorts, mysqlEnvs);

        // String namespace, V1Service body, String pretty, String dryRun, String fieldManager


//        apiVersion: v1
//        kind: Service
//        metadata:
//        name: powerjob-mysql
//        spec:
//        ports:
//        - name: mysql-port
//        port: 3306
//        protocol: TCP
//        targetPort: 3306
//        selector:
//        app: powerjob-mysql
//        type: ClusterIP
//
//        final CoreV1Api api, String namespace //
//                , EmbeddedPowerjobCoreDataSource.ServiceResName svcRes, TargetResName selector, Integer exportPort, String targetPortName

        K8SUtils.createService(v1Api, powerjobMySQLImage.namespace, K8S_DATAX_POWERJOB_MYSQL_SERVICE, K8S_DATAX_POWERJOB_MYSQL, mysqlPort3306, mysql3306);

//        V1Service svcBody = new V1Service();
//        svcBody.apiVersion(K8SUtils.REPLICATION_CONTROLLER_VERSION);
//        V1ObjectMeta meta = new V1ObjectMeta();
//        meta.setName(K8S_DATAX_POWERJOB_MYSQL_SERVICE.getName());
//        svcBody.setMetadata(meta);
//        V1ServiceSpec svcSpec = new V1ServiceSpec();
//        svcSpec.setType("ClusterIP");
//        svcSpec.setSelector(Collections.singletonMap(K8SUtils.LABEL_APP, K8S_DATAX_POWERJOB_MYSQL.getK8SResName()));
//
//        V1ServicePort svcPort = new V1ServicePort();
//        svcPort.setName("mysql-port");
//        svcPort.setTargetPort(new IntOrString(mysql3306));
//        svcPort.setPort(mysqlPort3306);
//        svcPort.setProtocol("TCP");
//        svcSpec.setPorts(Lists.newArrayList(svcPort));
//
//        svcBody.setSpec(svcSpec);
//
//        v1Api.createNamespacedService(powerjobMySQLImage.namespace, svcBody, K8SUtils.resultPrettyShow, null, null);
    }

    @TISExtension
    public static class DefaultDesc extends Descriptor<PowerjobCoreDataSource> {
        @Override
        public String getDisplayName() {
            return "Embedded";
        }
    }
}
