package com.qlangtech.tis.plugin.datax.powerjob.impl.serverport;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.powerjob.ServerPortExport;
import com.qlangtech.tis.realtime.utils.NetUtils;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1LoadBalancerIngress;
import io.kubernetes.client.openapi.models.V1Service;
import io.kubernetes.client.openapi.models.V1ServicePort;
import io.kubernetes.client.openapi.models.V1ServiceSpec;
import io.kubernetes.client.openapi.models.V1ServiceStatus;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberRange;

import java.util.Objects;
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
    public String getPowerjobExternalHost(CoreV1Api api, String nameSpace) {
        if (StringUtils.isEmpty(this.host)) {
            throw new IllegalStateException("prop host can not be empty");
        }
        return this.host + ":" + Objects.requireNonNull(nodePort, "node port can not be null");
    }

    @Override
    public void exportPort(String nameSpace, CoreV1Api api, String targetPortName) throws ApiException {
        super.exportPort(nameSpace, api, targetPortName);
        createService(nameSpace, api, targetPortName, this, (spec) -> {
            V1ServicePort servicePort = new V1ServicePort();
            servicePort.setNodePort(Objects.requireNonNull(this.nodePort, "nodePort can not be null"));
            return servicePort;
        });
    }

    @Override
    protected ServiceType getServiceType() {
        return ServiceType.NodePort;
    }


    public enum ServiceType {
        NodePort("NodePort", (svc) -> {
            throw new UnsupportedOperationException("nodePort is not support extrnalIp get process");
        }),
        LoadBalancer("LoadBalancer", (svc) -> {
            V1ServiceStatus status = svc.getStatus();

            for (V1LoadBalancerIngress ingress
                    : Objects.requireNonNull(status.getLoadBalancer(), "loadBalancer can not be null").getIngress()) {
                return ingress.getIp();
            }
            throw new IllegalStateException("can not find any ingress ip");
        }),
        ClusterIP("ClusterIP", (spec) -> {
            throw new UnsupportedOperationException(" ClusterIP is not support extrnalIp get process");
        });
        public final String token;
        private final Function<V1Service, String> externalIPSupplier;

        private ServiceType(String token, Function<V1Service, String> externalIPSupplier) {
            this.token = token;
            this.externalIPSupplier = externalIPSupplier;
        }

        public String getHost(V1Service svc, V1ServiceSpec spec, boolean clusterIP) {
            if (!this.token.equalsIgnoreCase(spec.getType())) {
                return null;
            }
            if (clusterIP) {
                return spec.getClusterIP();
            } else {
                return externalIPSupplier.apply(svc);
            }
        }
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
