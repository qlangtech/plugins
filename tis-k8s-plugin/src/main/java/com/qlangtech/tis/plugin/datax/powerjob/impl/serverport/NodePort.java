package com.qlangtech.tis.plugin.datax.powerjob.impl.serverport;

import com.alibaba.citrus.turbine.Context;
import com.google.gson.reflect.TypeToken;
import com.qlangtech.tis.config.k8s.impl.DefaultK8SImage;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.job.ServiceResName;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.powerjob.ServerPortExport;
import com.qlangtech.tis.plugin.k8s.K8SUtils;
import com.qlangtech.tis.realtime.utils.NetUtils;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1LoadBalancerIngress;
import io.kubernetes.client.openapi.models.V1OwnerReference;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1ServicePort;
import io.kubernetes.client.openapi.models.V1ServiceSpec;
import io.kubernetes.client.openapi.models.V1ServiceStatus;
import io.kubernetes.client.util.Watch;
import io.kubernetes.client.util.Watch.Response;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberRange;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/12/18
 */
public class NodePort extends ServerPortExport {

    public static final String KEY_HOST = "host";

    /**
     * 默认范围：30000-32767
     */
    @FormField(ordinal = 1, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer nodePort;

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.hostWithoutPort})
    public String host;

    @Override
    protected Integer getExportPort() {
        return this.serverPort;
    }

    @Override
    public <T> T accept(ServerPortExportVisitor<T> visitor) {
        return visitor.visit(this);
    }

    @Override
    public String getExternalHost(CoreV1Api api, DefaultK8SImage k8SImage, Pair<ServiceResName, TargetResName> serviceResAndOwner) {
        // String nameSpace = k8SImage.getNamespace();
        // k8SImage.internalClusterAvailable();
        if (StringUtils.isEmpty(this.host)) {
            throw new IllegalStateException("prop host can not be empty");
        }
        return this.host + ":" + Objects.requireNonNull(nodePort, "node port can not be null");
    }

    @Override
    public void exportPort(String nameSpace, CoreV1Api api, String targetPortName
            , Pair<ServiceResName, TargetResName> serviceResAndOwner, Optional<V1OwnerReference> ownerRef) throws ApiException {
        super.exportPort(nameSpace, api, targetPortName, serviceResAndOwner, ownerRef);
        createService(nameSpace, api, targetPortName, (spec) -> {
            V1ServicePort servicePort = new V1ServicePort();
            servicePort.setNodePort(Objects.requireNonNull(this.nodePort, "nodePort can not be null"));
            return servicePort;
        }, serviceResAndOwner, ownerRef);
    }

    @Override
    protected ServiceType getServiceType() {
        return ServiceType.NodePort;
    }


    public enum ServiceType {
        NodePort("NodePort", (svc) -> {
            throw new UnsupportedOperationException("nodePort is not support extrnalIp get process");
        }),
        LoadBalancer("LoadBalancer", (p) -> {
            V1Service svc = p.getRight();

            V1ServiceStatus status = svc.getStatus();

            return parseExternalIP(svc, p.getLeft(), true);


        }),
        ClusterIP("ClusterIP", (spec) -> {
            throw new UnsupportedOperationException(" ClusterIP is not support extrnalIp get process");
        });
        public final String token;
        private final Function<Pair<CoreV1Api, V1Service>, String> externalIPSupplier;

        private ServiceType(String token, Function<Pair<CoreV1Api, V1Service>, String> externalIPSupplier) {
            this.token = token;
            this.externalIPSupplier = externalIPSupplier;
        }

        public String getHost(Pair<CoreV1Api, V1Service> api, V1ServiceSpec spec, boolean clusterIP) {
            if (!this.token.equalsIgnoreCase(spec.getType())) {
                return null;
            }
            if (clusterIP) {
                return spec.getClusterIP();
            } else {
                return externalIPSupplier.apply(api);
            }
        }
    }


    private static String parseExternalIP(V1Service svc, CoreV1Api coreApi, boolean shallWatchChange) {
        V1ServiceStatus status = svc.getStatus();

        List<V1LoadBalancerIngress> ingressList
                = Objects.requireNonNull(status.getLoadBalancer(), "loadBalancer can not be null").getIngress();

        if (ingressList == null && shallWatchChange) {
            // 由于刚创建LoadBalancer，extrnalIP 还没有创建
            Watch<V1Service> rcWatch = null;
            try {
                rcWatch = Watch.createWatch(coreApi.getApiClient()
                        //
                        ,
                        //   K8SDataXPowerJobServer.K8S_DATAX_POWERJOB_SERVER_NODE_PORT_SERVICE.setFieldSelector(
                        coreApi.listNamespacedService(svc.getMetadata().getNamespace())
                                .pretty(K8SUtils.resultPrettyShow)
                                .watch(true)
                                .resourceVersion(svc.getMetadata().getResourceVersion())
                                .fieldSelector("metadata.name=" + svc.getMetadata().getName())
                                //)
                                .buildCall(K8SUtils.createApiCallback())
                        //
                        , new TypeToken<Response<V1Service>>() {
                        }.getType());

                for (Watch.Response<V1Service> event : rcWatch) {
                    String externalIP = null;
                    if ((externalIP = parseExternalIP(event.object, coreApi, false)) != null) {
                        return externalIP;
                    }
                }

            } catch (ApiException e) {
                throw new RuntimeException(e);
            } finally {
                try {
                    rcWatch.close();
                } catch (Throwable e) {

                }
            }
        }

        if (ingressList == null) {
            return null;
        }

        for (V1LoadBalancerIngress ingress : ingressList) {
            return ingress.getIp();
        }
        throw new IllegalStateException("can not find any ingress ip");
    }


    @TISExtension
    public static class DftDesc extends Descriptor<ServerPortExport> {
        public DftDesc() {
            super();
        }

        @Override
        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            return this.validateAll(msgHandler, context, postFormVals);
        }

        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {

            final NodePort portExport = postFormVals.newInstance();
            if (!NetUtils.isReachable(portExport.host)) {
                msgHandler.addFieldError(context, KEY_HOST, "不能连通");
                return false;
            }
            return true;
        }

        public boolean validateNodePort(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            NumberRange range = new NumberRange(30000, 32767);

            if (!range.containsNumber(Integer.parseInt(value))) {
                msgHandler.addFieldError(context, fieldName, "必须符合范围" + range);
                return false;
            }

            return true;
        }


        @Override
        public String getDisplayName() {
            return NodePort.class.getSimpleName();
        }
    }
}
