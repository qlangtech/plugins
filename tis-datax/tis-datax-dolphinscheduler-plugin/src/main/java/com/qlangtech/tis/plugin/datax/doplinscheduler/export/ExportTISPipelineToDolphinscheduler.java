/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qlangtech.tis.plugin.datax.doplinscheduler.export;

import com.alibaba.citrus.turbine.Context;
import com.google.common.collect.Lists;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.datax.DefaultDataXProcessorManipulate;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor.ParseDescribable;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.lang.TisException;
import com.qlangtech.tis.manage.IAppSource;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.KeyedPluginStore;
import com.qlangtech.tis.plugin.StoreResourceType;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.BasicDistributedSPIDataXJobSubmit;
import com.qlangtech.tis.plugin.datax.WorkflowSPIInitializer;
import com.qlangtech.tis.plugin.datax.doplinscheduler.DSWorkflowInstance;
import com.qlangtech.tis.plugin.datax.doplinscheduler.DSWorkflowPayload;
import com.qlangtech.tis.plugin.datax.doplinscheduler.DolphinschedulerDistributedSPIDataXJobSubmit;
import com.qlangtech.tis.plugin.datax.doplinscheduler.export.DolphinSchedulerURLBuilder.DolphinSchedulerResponse;
import com.qlangtech.tis.plugin.ds.manipulate.ManipuldateUtils;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.util.IPluginContext;
import com.qlangtech.tis.util.IPluginItemsProcessor;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-08-16 08:29
 **/
public class ExportTISPipelineToDolphinscheduler extends DefaultDataXProcessorManipulate implements IdentityName {

    private static final String FIELD_DS_ENDPOINT = "dsEndpoint";
    private static final String FIELD_PROCESS_NAME = "processName";
    private static final String FIELD_PROJECT_CODE = "projectCode";

    @FormField(ordinal = 1, identity = true, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
    public String processName;

    @FormField(ordinal = 2, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String dsEndpoint;

    @FormField(ordinal = 3, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.integer})
    public String projectCode;


    @FormField(ordinal = 5, type = FormFieldType.TEXTAREA, validate = {})
    public String processDescription;

    @FormField(ordinal = 200, type = FormFieldType.MULTI_SELECTABLE, validate = {Validator.require})
    public List<IdentityName> targetTables = Lists.newArrayList();

//    public List<Header> appendToken(List<Header> originHeaders) {
//        List<Header> headers = Lists.newArrayList(originHeaders);
//        headers.add(new Header("token", getDSEndpoint().serverToken));
//        headers.add(new Header("Accept", "application/json"));
//        return headers;
//    }

    /**
     * 117442916207136
     *
     * @return
     */
    public DolphinSchedulerURLBuilder processDefinition() {
        return getDSEndpoint().createSchedulerURLBuilder().appendSubPath("projects", this.projectCode, "process-definition");
//        boolean endWithSlash = StringUtils.endsWith(getDSEndpoint().serverPath, "/");
//        StringBuffer url = new StringBuffer(getDSEndpoint().serverPath);
//        if (!endWithSlash) {
//            url.append("/");
//        }
//        return url.append("projects/").append(this.projectCode).append("/process-definition");
        //    return new StringBuffer("http://192.168.28.201:12345/dolphinscheduler/projects/" + this.projectCode + "/process-definition");
    }

    @Override
    public String identityValue() {
        return this.processName;
    }

    public DolphinSchedulerEndpoint getDSEndpoint() {
        DolphinSchedulerEndpoint dsEndpoint = ParamsConfig.getItem(this.dsEndpoint, DolphinSchedulerEndpoint.DISPLAY_NAME);
        Objects.requireNonNull(dsEndpoint, "dsEndpoint can not be null,dsEndpoint:" + this.dsEndpoint);
        return dsEndpoint;
    }


    @Override
    public void manipuldateProcess(IPluginContext pluginContext, Optional<Context> context) {
        // 将TIS的数据同步通道配置同步到DS中
        String[] originId = new String[1];
        /**
         * 校验
         */
        IPluginItemsProcessor itemsProcessor
                = ManipuldateUtils.cloneInstance(pluginContext, context.get(), this.identityValue()
                , (meta) -> {
                }, (oldIdentityId) -> {
                    originId[0] = oldIdentityId;
                });
        if (StringUtils.isEmpty(originId[0])) {
            throw new IllegalStateException("originId can not be null");
        }
        if (itemsProcessor == null) {
            return;
        }

        Pair<List<ExportTISPipelineToDolphinscheduler>, IPluginStore<DefaultDataXProcessorManipulate>>
                pair = DefaultDataXProcessorManipulate.loadPlugins(pluginContext, ExportTISPipelineToDolphinscheduler.class, originId[0]);

        // IPluginStore<DefaultDataXProcessorManipulate> pluginStore = getPluginStore(pluginContext, originId[0]);
        // 查看是否已经有保存的实例
        //  List<ExportTISPipelineToDolphinscheduler> export2DSCfgs = pair.getLeft();

        if (pair.getLeft().size() > 0) {
            for (ExportTISPipelineToDolphinscheduler i : pair.getLeft()) {
                pluginContext.addErrorMessage(context.get(), "实例'" + i.processName + "'已经配置，不能再创建新实例");
            }
            //  throw TisException.create("实例已经配置不能重复创建");
            return;
        }

        IDataxProcessor dataxProcessor = DataxProcessor.load(pluginContext, originId[0]);
        DSWorkflowPayload workflowPayload = new DSWorkflowPayload(this, dataxProcessor
                , BasicDistributedSPIDataXJobSubmit.getCommonDAOContext(pluginContext), new DolphinschedulerDistributedSPIDataXJobSubmit());
        WorkflowSPIInitializer<DSWorkflowInstance> workflowSPIInitializer = new WorkflowSPIInitializer<>(workflowPayload);
        /**
         *1. 将配置同步到DS
         */
        DSWorkflowInstance wfInstance = workflowSPIInitializer.initialize(true);


        /**
         *2. 并且将实例持久化在app管道下，当DS端触发会调用 DolphinschedulerDistributedSPIDataXJobSubmit.createPayload()方法获取DS端的WorkflowDAG拓扑视图
         */
        pair.getRight().setPlugins(pluginContext, context, Collections.singletonList(new ParseDescribable(this)));
    }

//    private static IPluginStore<DefaultDataXProcessorManipulate> getPluginStore(
//            IPluginContext context, String appName) {
//        if (StringUtils.isEmpty(appName)) {
//            throw new IllegalArgumentException("param appName can not be empty");
//        }
//        KeyedPluginStore.AppKey appKey = new KeyedPluginStore.AppKey(context, StoreResourceType.DataApp, appName, DefaultDataXProcessorManipulate.class);
//        IPluginStore<DefaultDataXProcessorManipulate> pluginStore = TIS.getPluginStore(appKey);
//        return pluginStore;
//    }

    @TISExtension
    public static class DefaultDesc extends DefaultDataXProcessorManipulate.BasicDesc
            implements IEndTypeGetter, FormFieldType.IMultiSelectValidator {
        public DefaultDesc() {
            super();
            this.registerSelectOptions(FIELD_DS_ENDPOINT, () -> ParamsConfig.getItems(DolphinSchedulerEndpoint.DISPLAY_NAME));
        }

        @Override
        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            return this.validateAll(msgHandler, context, postFormVals);
        }

        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {

            ExportTISPipelineToDolphinscheduler export = postFormVals.newInstance();
            //http://192.168.28.201:12345/dolphinscheduler/swagger-ui/index.html?language=zh_CN&lang=cn#/%E6%B5%81%E7%A8%8B%E5%AE%9A%E4%B9%89%E7%9B%B8%E5%85%B3%E6%93%8D%E4%BD%9C/verifyProcessDefinitionName
            DolphinSchedulerResponse response = export.processDefinition()
                    .appendSubPath("verify-name")
                    .appendQueryParam("name", export.processName).applyGet();

            if (!response.isSuccess()) {
                Status status = Status.findStatusBy(response.getCode());
                switch (status) {
                    case PROCESS_DEFINITION_NAME_EXIST:
                        msgHandler.addFieldError(context, FIELD_PROCESS_NAME, response.getMessage());
                        break;
                    case PROJECT_NOT_EXIST:
                        msgHandler.addFieldError(context, FIELD_PROJECT_CODE, response.getMessage());
                        break;
                    default:
                        throw new IllegalStateException("illegal status:" + status);
                }
                return false;
            }
//            new StreamProcess<Void>() {
//                @Override
//                public Void p(int status, InputStream stream, Map<String, List<String>> headerFields) throws IOException {
//                    JSONObject result = JSONObject.parseObject(IOUtils.toString(stream, TisUTF8.get()));
//                    if (!result.getBooleanValue("success")) {
//                        msgHandler.addFieldError(context, FIELD_PROCESS_NAME, result.getString("msg"));
//                        return null;
//                    }
//
//                    return null;
//                }
//            }

            return true;
        }

        @Override
        public EndType getEndType() {
            return EndType.Dolphinscheduler;
        }

        @Override
        public String getDisplayName() {
            return "Export To Dolphinscheduler";
        }
    }
}
