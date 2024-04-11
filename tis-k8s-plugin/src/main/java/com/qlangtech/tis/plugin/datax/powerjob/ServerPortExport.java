package com.qlangtech.tis.plugin.datax.powerjob;

import com.qlangtech.tis.config.k8s.impl.DefaultK8SImage;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.job.ServiceResName;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.powerjob.impl.serverport.Ingress;
import com.qlangtech.tis.plugin.datax.powerjob.impl.serverport.LoadBalance;
import com.qlangtech.tis.plugin.datax.powerjob.impl.serverport.NodePort;
import com.qlangtech.tis.plugin.datax.powerjob.impl.serverport.NodePort.ServiceType;
import com.qlangtech.tis.plugin.k8s.K8SUtils;
import com.qlangtech.tis.util.DescriptorsJSONResult;
import io.kubernetes.client.custom.IntOrString;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1ServiceList;
import io.kubernetes.client.openapi.models.V1ServicePort;
import io.kubernetes.client.openapi.models.V1ServiceSpec;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/12/18
 * @see Ingress
 * @see LoadBalance
 * @see NodePort
 */
public abstract class ServerPortExport implements Describable<ServerPortExport> {

    /**
     * 容器端口
     */
    @FormField(ordinal = 0, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer serverPort;

    private transient String clusterIP;

//    /**
//     * 是否使用clusterIP，当TIS和Powerjob在同一个K8S的网络中，可以使用clusterIP作为连接地址，如不在同一个网络中需要使用extrnalIP地址
//     */
//    @FormField(ordinal = 1, advance = true, type = FormFieldType.ENUM, validate = {Validator.require})
//    public Boolean usingClusterIP;

    public static Integer dftExportPort() {
        return ((DefaultExportPortProvider) DescriptorsJSONResult.getRootDescInstance()).get();
    }


    public abstract <T> T accept(ServerPortExportVisitor<T> visitor);

    public interface ServerPortExportVisitor<T> {
        public T visit(Ingress ingress);

        public T visit(LoadBalance loadBalance);

        public T visit(NodePort nodePort);
    }


    /**
     * 提供Describer支持默认端口值
     */
    public interface DefaultExportPortProvider extends Supplier<Integer> {

//        /**
//         * 默认是否使用clusterIP作为用户暴露终端
//         *
//         * @return
//         */
//        public Boolean dftUsingClusterIP();

    }


    /**
     * 服务向外暴露的端口
     *
     * @return
     */
    protected abstract Integer getExportPort();


    public static String getHost(CoreV1Api api, String nameSpace, ServiceType serviceType, ServiceResName svcRes, boolean clusterIP) {
        String resultHost = null;
        V1ServiceList svcList = null;
        try {

//            svcList = Objects.requireNonNull(api, "coreApi can not be null")
//                    .listNamespacedService(nameSpace)
//                    .pretty(K8SUtils.resultPrettyShow)
//                    .fieldSelector("metadata.name=" + K8S_DATAX_POWERJOB_SERVER_NODE_PORT_SERVICE.getName())
//                    .execute();

            svcList = Objects.requireNonNull(api, "coreApi can not be null")
                    .listNamespacedService(nameSpace)
                    .pretty(K8SUtils.resultPrettyShow)
                    .fieldSelector("metadata.name=" + svcRes.getName())
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

    /**
     * @param nameSpace
     * @param api
     * @param targetPortName
     * @param serviceResAndOwner example:Pair.of(K8S_DATAX_POWERJOB_SERVER_NODE_PORT_SERVICE, K8S_DATAX_POWERJOB_SERVER)
     * @throws ApiException
     */
    public void exportPort(String nameSpace, CoreV1Api api
            , String targetPortName, Pair<ServiceResName, TargetResName> serviceResAndOwner, Optional<V1OwnerReference> ownerRef) throws ApiException {
        this.clusterIP = null;
    }

    protected ServiceResName createService(String nameSpace, CoreV1Api api //
            , String targetPortName
            , Function<V1ServiceSpec, V1ServicePort> specCreator
            , Pair<ServiceResName, TargetResName> serviceResAndOwner, Optional<V1OwnerReference> ownerRef) throws ApiException {

        V1ServiceSpec svcSpec = new V1ServiceSpec();
        svcSpec.setType(this.getServiceType().token);
        V1ServicePort servicePort = specCreator.apply(svcSpec);
        final IntOrString containerPort = new IntOrString(this.serverPort);
        //  Optional<V1OwnerReference> ownerRef = Optional.empty();
        return K8SUtils.createService(api, nameSpace
                , Objects.requireNonNull(serviceResAndOwner, "serviceResAndOwner can not be null").getKey()
                , serviceResAndOwner.getRight(), this.getExportPort(), targetPortName, containerPort, ownerRef, svcSpec, servicePort);
    }

//    protected Pair<ServiceResName, TargetResName> getServiceResAndOwner() {
//        return Pair.of(K8S_DATAX_POWERJOB_SERVER_NODE_PORT_SERVICE, K8S_DATAX_POWERJOB_SERVER);
//    }

    public final String getClusterHost(
            CoreV1Api api, DefaultK8SImage k8SImage, Pair<ServiceResName, TargetResName> serviceResAndOwner) {
        return getClusterHost(api, k8SImage, serviceResAndOwner, false);
    }

    /**
     * 内部集群可用host地址
     *
     * @param api
     * @param k8SImage
     * @return
     */
    public final String getClusterHost(
            CoreV1Api api, DefaultK8SImage k8SImage, Pair<ServiceResName, TargetResName> serviceResAndOwner, boolean forceExternalHost) {
        String nameSpace = k8SImage.getNamespace();
        if (forceExternalHost || !k8SImage.internalClusterAvailable()) {
            return getExternalHost(api, k8SImage, serviceResAndOwner);
        }
        if (clusterIP == null) {
            //Pair<ServiceResName, TargetResName> serviceResAndOwner = getServiceResAndOwner();
            this.clusterIP = getHost(api, nameSpace, getServiceType(), serviceResAndOwner.getKey(), true);
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
    public abstract String getExternalHost(
            CoreV1Api api, DefaultK8SImage k8SImage, Pair<ServiceResName, TargetResName> serviceResAndOwner);
}
