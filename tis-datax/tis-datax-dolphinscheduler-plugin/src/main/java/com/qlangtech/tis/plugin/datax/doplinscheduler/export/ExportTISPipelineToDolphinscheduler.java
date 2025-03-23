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
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.datax.DefaultDataXProcessorManipulate;
import com.qlangtech.tis.datax.IDataXTaskRelevant;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.extension.Descriptor.ParseDescribable;
import com.qlangtech.tis.extension.IDescribableManipulate.IManipulateStorable;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.manage.common.AppAndRuntime;
import com.qlangtech.tis.manage.common.Config;
import com.qlangtech.tis.manage.common.HttpUtils.PostParam;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.MemorySpecification;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.BasicDistributedSPIDataXJobSubmit;
import com.qlangtech.tis.plugin.datax.WorkflowSPIInitializer;
import com.qlangtech.tis.plugin.datax.doplinscheduler.DSWorkflowInstance;
import com.qlangtech.tis.plugin.datax.doplinscheduler.DSWorkflowPayload;
import com.qlangtech.tis.plugin.datax.doplinscheduler.DolphinschedulerDistributedSPIDataXJobSubmit;
import com.qlangtech.tis.plugin.datax.doplinscheduler.export.DolphinSchedulerURLBuilder.DolphinSchedulerResponse;
import com.qlangtech.tis.plugin.ds.manipulate.ManipulateItemsProcessor;
import com.qlangtech.tis.plugin.ds.manipulate.ManipuldateUtils;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-08-16 08:29
 **/
public class ExportTISPipelineToDolphinscheduler extends DefaultDataXProcessorManipulate implements IdentityName {

    private static final String FIELD_DS_ENDPOINT = "dsEndpoint";
    private static final String FIELD_PROCESS_NAME = "processName";
    public static final String FIELD_PROJECT_CODE = "projectCode";


    @FormField(ordinal = 1, identity = true, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity, Validator.forbid_start_with_number})
    public String processName;

    @FormField(ordinal = 2, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String dsEndpoint;

    @FormField(ordinal = 3, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.integer})
    public String projectCode;
    /**
     * 是否在TIS端生成执行历史记录
     */
    @FormField(ordinal = 4, type = FormFieldType.ENUM, validate = {Validator.require})
    public Boolean createHistory;
    /**
     * 设置TIS回调参数集
     */
    @FormField(ordinal = 6, validate = {Validator.require})
    public DSTISCallback callback;

    public static final String dftProcessName() {
        AppAndRuntime appAndRuntime = Objects.requireNonNull(AppAndRuntime.getAppAndRuntime(), "appAndRuntime can not be null");
        return appAndRuntime.getAppName() + "_pipeline_from_tis";
    }

    /**
     * 目标表选择
     */
    @FormField(ordinal = 200, validate = {Validator.require})
    public DSTargetTables target;

    @FormField(ordinal = 201, advance = true, type = FormFieldType.INPUTTEXT, validate = {Validator.none_blank, Validator.absolute_path})
    public String deployDir;
    @FormField(ordinal = 202, advance = true, type = FormFieldType.TEXTAREA, validate = {})
    public String processDescription;

    @FormField(ordinal = 203, advance = true, validate = {Validator.require})
    public DSTaskGroup taskGroup;

    @FormField(ordinal = 204, advance = true, validate = {Validator.require})
    public MemorySpecification memorySpec;


    /**
     * 117442916207136
     *
     * @return
     */
    public DolphinSchedulerURLBuilder processDefinition() {
        return getDSEndpoint().createSchedulerURLBuilder()
                .appendSubPath("projects", Objects.requireNonNull(this.projectCode, "projectCode can not be null"), "process-definition");
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
        // String[] originId = new String[1];
        /**
         * 校验
         */
        ManipulateItemsProcessor itemsProcessor
                = ManipuldateUtils.instance(pluginContext, context.get(), null
                , (meta) -> {
                });
        if (StringUtils.isEmpty(itemsProcessor.getOriginIdentityId())) {
            throw new IllegalStateException("originId can not be null");
        }
        if (itemsProcessor == null) {
            return;
        }

        Pair<List<ExportTISPipelineToDolphinscheduler>, IPluginStore<DefaultDataXProcessorManipulate>>
                pair = DefaultDataXProcessorManipulate.loadPlugins(pluginContext, ExportTISPipelineToDolphinscheduler.class, itemsProcessor.getOriginIdentityId());

        /**
         * 是否需要删除
         */
        if (itemsProcessor.isDeleteProcess()) {
            // 只删除TIS本地端配置，dolphinscheduler端不进行任何操作
            pair.getRight().setPlugins(pluginContext, context, Collections.emptyList());
            return;
        }


        if (!itemsProcessor.isUpdateProcess()) {
            // 添加操作
            if (pair.getLeft().size() > 0) {
                for (ExportTISPipelineToDolphinscheduler i : pair.getLeft()) {
                    pluginContext.addErrorMessage(context.get(), "实例'" + i.processName + "'已经配置，不能再创建新实例");
                }
                //  throw TisException.create("实例已经配置不能重复创建");
                return;
            }
        }

        /**===============================
         * 添加项目参数
         ===============================*/
        addProjectParameters();

//        /**===============================
//         * 为执行任务添加task工作组
//         ===============================*/
//        TaskGroup taskGroup = taskGroup.addTaskGroup(this);

        // IPluginStore<DefaultDataXProcessorManipulate> pluginStore = getPluginStore(pluginContext, originId[0]);
        // 查看是否已经有保存的实例
        //  List<ExportTISPipelineToDolphinscheduler> export2DSCfgs = pair.getLeft();


        IDataxProcessor dataxProcessor = DataxProcessor.load(pluginContext, itemsProcessor.getOriginIdentityId());
        DSWorkflowPayload workflowPayload = new DSWorkflowPayload(this, dataxProcessor
                , BasicDistributedSPIDataXJobSubmit.getCommonDAOContext(pluginContext), new DolphinschedulerDistributedSPIDataXJobSubmit());
        WorkflowSPIInitializer<DSWorkflowInstance> workflowSPIInitializer = new WorkflowSPIInitializer<>(workflowPayload);
        /**
         * 1. 将配置同步到DS
         */
        DSWorkflowInstance wfInstance = workflowSPIInitializer.initialize(true, itemsProcessor.isUpdateProcess());


        /**
         *2. 并且将实例持久化在app管道下，当DS端触发会调用 DolphinschedulerDistributedSPIDataXJobSubmit.createPayload()方法获取DS端的WorkflowDAG拓扑视图
         */
        pair.getRight().setPlugins(pluginContext, context, Collections.singletonList(new ParseDescribable(this)));
    }


    void addProjectParameters() {
        DolphinSchedulerEndpoint endpoint = this.getDSEndpoint();
        DolphinSchedulerResponse response = endpoint.createSchedulerURLBuilder()
                .appendSubPath("projects", this.projectCode, "project-parameter")
                .appendPageParam()
//                .appendQueryParam(DolphinSchedulerURLBuilder.FIELD_PAGE_NO, 1)
//                .appendQueryParam(DolphinSchedulerURLBuilder.FIELD_PAGE_SIZE, 100)
                .applyGet();
        if (!response.isSuccess()) {
            throw new IllegalStateException(response.errorDescribe());
        }

        /**
         * <pre> example
         *     {
         * 	"totalList": [{
         * 		"modifyUser": "admin",
         * 		"code": 117443106776544,
         * 		"projectCode": 117442916207136,
         * 		"createTime": "2024-08-20 10:28:54",
         * 		"updateTime": "2024-08-30 18:49:27",
         * 		"createUser": "admin",
         * 		"id": 1,
         * 		"paramName": "tisHost",
         * 		"userId": 1,
         * 		"operator": 1,
         * 		"paramValue": "192.168.28.107"
         *        }],
         * 	"total": 1,
         * 	"totalPage": 1,
         * 	"pageNo": 0,
         * 	"pageSize": 100,
         * 	"currentPage": 1
         * }
         * </pre>
         */
        JSONObject data = response.getData();
        JSONArray totals = data.getJSONArray("totalList");
        Set<String> paramNames = Sets.newHashSet();
        for (Object o : totals) {
            JSONObject sysCfg = (JSONObject) o;
            paramNames.add(sysCfg.getString("paramName"));
        }

        DSTISCallback tisCallbackParams = Objects.requireNonNull(this.callback, "callback can not be null");

        if (!paramNames.contains(Config.KEY_TIS_HTTP_Host)) {
            createProjectParam(endpoint, Config.KEY_TIS_HTTP_Host, tisCallbackParams.tisHTTPHost);
        }

        if (!paramNames.contains(Config.KEY_TIS_ADDRESS)) {
            createProjectParam(endpoint, Config.KEY_TIS_ADDRESS, tisCallbackParams.tisAddress);
        }

        if (StringUtils.isNotEmpty(this.deployDir)) {
            if (!paramNames.contains(IDataXTaskRelevant.KEY_TIS_DATAX_WORK_DIR)) {
                createProjectParam(endpoint, IDataXTaskRelevant.KEY_TIS_DATAX_WORK_DIR, this.deployDir);
            }
        }

    }

    /**
     * 创建系统参数
     *
     * @param endpoint
     * @param keyName
     * @param value
     */
    private void createProjectParam(DolphinSchedulerEndpoint endpoint, String keyName, String value) {
        if (StringUtils.isEmpty(value) || StringUtils.isEmpty(keyName)) {
            throw new IllegalArgumentException("key:" + keyName + " val:" + value + " neither of them can be empty");
        }
        List<PostParam> params = Lists.newArrayList();
        params.add(new PostParam("projectParameterName", keyName));
        params.add(new PostParam("projectParameterValue", value));
        DolphinSchedulerResponse response = Objects.requireNonNull(endpoint, "endpoint can not be null")
                .createSchedulerURLBuilder()
                .appendSubPath("projects", this.projectCode, "project-parameter")
                .applyPost(params, Optional.empty());
        if (!response.isSuccess()) {
            throw new IllegalStateException("create system param,key:" + keyName + ",value:" + value
                    + response.errorDescribe());
        }
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
            implements IEndTypeGetter, IManipulateStorable {
        public DefaultDesc() {
            super();
            this.registerSelectOptions(FIELD_DS_ENDPOINT, () -> ParamsConfig.getItems(DolphinSchedulerEndpoint.DISPLAY_NAME));
        }

        @Override
        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            return this.validateAll(msgHandler, context, postFormVals);
        }

        @Override
        public boolean isManipulateStorable() {
            return true;
        }

        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {

            ManipulateItemsProcessor itemsProcessor
                    = ManipuldateUtils.instance((IPluginContext) msgHandler, context, null
                    , (meta) -> {
                    });
            ExportTISPipelineToDolphinscheduler export = postFormVals.newInstance();

            if (itemsProcessor == null) {
                return false;
            }

            if (itemsProcessor.isDeleteProcess()) {
                return true;
            }

            if (itemsProcessor.isUpdateProcess()) {
                /********************************************************
                 * 更新流程，需要确保definitionName已经存在
                 ********************************************************/
                DolphinSchedulerResponse response = export.processDefinition()
                        .appendSubPath("query-by-name")
                        .appendQueryParam("name", export.processName).applyGet();
                if (!response.isSuccess()) {
                    Status status = Status.findStatusBy(response.getCode());
                    switch (status) {
                        case PROCESS_DEFINE_NOT_EXIST: {
                            msgHandler.addFieldError(context, FIELD_PROCESS_NAME, response.getMessage());
                            break;
                        }
                        default:
                            throw new IllegalStateException("illegal status:" + response.errorDescribe());
                    }
                    return false;
                }
                JSONObject data = response.getData();
                String releaseState = data.getJSONObject("processDefinition").getString("releaseState");
                if (!"OFFLINE".equalsIgnoreCase(releaseState)) {
                    msgHandler.addFieldError(context, FIELD_PROCESS_NAME, "DS中对应工作流实例需要是‘下线’状态");
                    return false;
                }
            } else {
                // 新增要校验名字不冲突
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
                            throw new IllegalStateException("illegal status:" + response.errorDescribe());
                    }
                    return false;
                }
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
