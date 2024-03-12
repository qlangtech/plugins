package com.qlangtech.tis.plugin.datax.powerjob;

import com.qlangtech.tis.datax.job.ServiceResName;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.powerjob.impl.serverport.NodePort.ServiceType;
import com.qlangtech.tis.plugin.k8s.K8SUtils;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1ServiceList;
import io.kubernetes.client.openapi.models.V1ServicePort;
import io.kubernetes.client.openapi.models.V1ServiceSpec;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Objects;
import java.util.function.Function;

import static com.qlangtech.tis.plugin.datax.powerjob.K8SDataXPowerJobServer.K8S_DATAX_POWERJOB_SERVER;
import static com.qlangtech.tis.plugin.datax.powerjob.K8SDataXPowerJobServer.K8S_DATAX_POWERJOB_SERVER_NODE_PORT_SERVICE;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/12/18
 */
public abstract class ServerPortExport implements Describable<ServerPortExport> {

    @FormField(ordinal = 0, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer serverPort;

    private transient String clusterIP;

    /**
     * 是否使用clusterIP，当TIS和Powerjob在同一个K8S的网络中，可以使用clusterIP作为连接地址，如不在同一个网络中需要使用extrnalIP地址
     */
    @FormField(ordinal = 1, advance = true, type = FormFieldType.ENUM, validate = {Validator.require})
    public Boolean usingClusterIP;

    public static String getHost(CoreV1Api api, String nameSpace, ServiceType serviceType, boolean clusterIP) {
        String resultHost = null;
        V1ServiceList svcList = null;
        try {
//            svcList = Objects.requireNonNull(api, "coreApi can not be null")
//                    .listNamespacedService(nameSpace, K8SUtils.resultPrettyShow, null, null
//                            , "metadata.name=" + K8S_DATAX_POWERJOB_SERVER_NODE_PORT_SERVICE.getName()
//                            , null//"app=" + K8S_DATAX_POWERJOB_SERVER.getK8SResName()
//                            , null, null, null, null);

            svcList = Objects.requireNonNull(api, "coreApi can not be null")
                    .listNamespacedService(nameSpace)
                    .pretty(K8SUtils.resultPrettyShow)
                    .fieldSelector("metadata.name=" + K8S_DATAX_POWERJOB_SERVER_NODE_PORT_SERVICE.getName())
                    .execute();

            for (V1Service svc : svcList.getItems()) {
                resultHost = serviceType.getHost(Pair.of(api, svc), svc.getSpec(), clusterIP);
                if (StringUtils.isNotEmpty(resultHost)) {
                    return resultHost;
                }
            }
        } catch (ApiException e) {
            throw new RuntimeException(e);
        }
        if (org.apache.commons.lang3.StringUtils.isEmpty(resultHost)) {
            throw new IllegalStateException("LoadBalancer host can not be null, response Detail:" + String.valueOf(svcList));
        }
        return resultHost;

    }

    public void exportPort(String nameSpace, CoreV1Api api, String targetPortName) throws ApiException {
        this.clusterIP = null;
    }

    protected ServiceResName createService(String nameSpace, CoreV1Api api //
            , String targetPortName, ServerPortExport portExport
            , Function<V1ServiceSpec, V1ServicePort> specCreator) throws ApiException {

        V1ServiceSpec svcSpec = new V1ServiceSpec();
        svcSpec.setType(this.getServiceType().token);
        V1ServicePort servicePort = specCreator.apply(svcSpec);

        return K8SUtils.createService(api, nameSpace
                , K8S_DATAX_POWERJOB_SERVER_NODE_PORT_SERVICE
                , K8S_DATAX_POWERJOB_SERVER, portExport.serverPort, targetPortName, svcSpec, servicePort);
    }


    /**
     * 内部集群可用host地址
     *
     * @param api
     * @param nameSpace
     * @return
     */
    public final String getPowerjobClusterHost(CoreV1Api api, String nameSpace) {

        if (!this.usingClusterIP) {
            return getPowerjobExternalHost(api, nameSpace);
        }
        if (clusterIP == null) {
            this.clusterIP = getHost(api, nameSpace, getServiceType(), true);
        }
        return this.clusterIP + ":" + Objects.requireNonNull(serverPort, "serverPort  can not be null");
    }

    /**
     * 暴露服务的服务类型
     *
     * @return
     */
    protected abstract ServiceType getServiceType();

    /**
     * TIS 可用的 host:port 访问地址
     *
     * @return
     */
    //  public abstract String getPowerjobHost();
    public abstract String getPowerjobExternalHost(CoreV1Api api, String nameSpace);
}
