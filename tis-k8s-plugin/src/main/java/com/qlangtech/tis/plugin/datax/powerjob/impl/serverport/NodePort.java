package com.qlangtech.tis.plugin.datax.powerjob.impl.serverport;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.datax.job.IRegisterApp;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.ExtensionList;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.powerjob.K8SDataXPowerJobServer;
import com.qlangtech.tis.plugin.datax.powerjob.ServerPortExport;
import com.qlangtech.tis.plugin.k8s.K8SUtils;
import com.qlangtech.tis.realtime.utils.NetUtils;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import io.kubernetes.client.openapi.models.V1ServicePort;
import io.kubernetes.client.openapi.models.V1ServiceSpec;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.math.NumberRange;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Objects;

import static com.qlangtech.tis.plugin.datax.powerjob.K8SDataXPowerJobServer.K8S_DATAX_POWERJOB_SERVER;
import static com.qlangtech.tis.plugin.datax.powerjob.K8SDataXPowerJobServer.K8S_DATAX_POWERJOB_SERVER_NODE_PORT_SERVICE;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/12/18
 */
public class NodePort extends ServerPortExport {

    private static final String KEY_HOST = "host";

    /**
     * 默认范围：30000-32767
     */
    @FormField(ordinal = 1, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer nodePort;

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.hostWithoutPort})
    public String host;

    @Override
    public String getPowerjobHost() {
        if (StringUtils.isEmpty(this.host)) {
            throw new IllegalStateException("prop host can not be empty");
        }
        return host + ":" + Objects.requireNonNull(nodePort, "node port can not be null");
    }

    @Override
    public void exportPort(String nameSpace, CoreV1Api api, String targetPortName) throws ApiException {
        K8SUtils.createService(api, nameSpace
                , K8S_DATAX_POWERJOB_SERVER_NODE_PORT_SERVICE, K8S_DATAX_POWERJOB_SERVER, this.serverPort, targetPortName, () -> {
                    V1ServiceSpec svcSpec = new V1ServiceSpec();
                    svcSpec.setType("NodePort");
                    //svcSpec.setType("LoadBalancer");
                    V1ServicePort servicePort = new V1ServicePort();
                    servicePort.setNodePort(Objects.requireNonNull(this.nodePort, "nodePort can not be null"));
                    return Pair.of(svcSpec, servicePort);
                });
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
