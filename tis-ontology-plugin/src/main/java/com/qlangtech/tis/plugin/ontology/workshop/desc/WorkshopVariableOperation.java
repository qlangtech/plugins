package com.qlangtech.tis.plugin.ontology.workshop.desc;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.manage.common.IAjaxResult;
import com.qlangtech.tis.plugin.ontology.workshop.WorkshopModule;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopVariable;
import com.qlangtech.tis.plugin.ontology.workshop.service.WorkshopModuleService;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.util.AttrValMap;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

/**
 * Workshop Variable 自己的 HTTP 端点（list / get / create / update / delete）。
 *
 * <h3>为什么是一个独立的描述符类</h3>
 * 端点的寻址方式是 {@code PluginAction.doDescriptionProcess()} 按 {@code impl} 参数
 * {@code TIS.get().getDescriptor(String)} 找 descriptor，而 {@code Descriptor.getId()}
 * 返回的是其 {@code clazz} 的 FQCN。原先该端点挂在 {@code WorkshopVariable.DefaultDescriptor}
 * 上（{@code impl=...workshop.model.WorkshopVariable}）。
 *
 * <p>现在 {@link WorkshopVariable} 是抽象基类、12 个变量类型子类各自持有自己的
 * {@code DefaultDescriptor}，这个位置上不再有描述符 —— 于是把 CRUD 摘出来放在本类。
 * 顺带的好处是它与 12 个插件描述符彻底解耦：
 * {@code getDescriptorList(WorkshopVariable.class)}（前端加载类型选择器走的接口）
 * 只会返回那 12 个子类，不会有第 13 个"不是类型"的条目混进去。
 *
 * <p>本类<b>不是</b>一个配置实体，没有任何 {@code @FormField}，它的存在只是为了承载
 * {@link DefaultDescriptor} 从而拿到一个稳定的 {@code impl} FQCN。
 * 前端常量见 tis-console 的 {@code workshop-variable-api.service.ts}。
 *
 * @see WorkshopVariable
 */
public class WorkshopVariableOperation implements Describable<WorkshopVariableOperation> {

    private static final Logger logger = LoggerFactory.getLogger(WorkshopVariableOperation.class);

    @TISExtension
    public static class DefaultDescriptor extends Descriptor<WorkshopVariableOperation> {

        @Override
        public String getDisplayName() {
            return "Workshop Variable Operation";
        }

        /**
         * 按 type 参数分派变量的增删改查。
         *
         * <pre>
         * POST /coredefine/corenodemanage.ajax
         *   ?event_submit_do_description_process=y
         *   &amp;action=plugin_action
         *   &amp;impl=com.qlangtech.tis.plugin.ontology.workshop.desc.WorkshopVariableOperation
         *   &amp;type=create | get | list | update | delete
         *   &amp;ontologyDomainId={domainId}
         *   &amp;moduleName={moduleName}
         *   &amp;variableId={variableId}          // get | update | delete 需要
         *   &amp;variable={impl,vals}             // create | update 需要
         * </pre>
         *
         * @see Descriptor#httpProcess(IControlMsgHandler, IPluginContext, Context)
         */
        @Override
        public void httpProcess(IControlMsgHandler msgHandler, IPluginContext pluginContext, Context context) throws Exception {
            String type = msgHandler.getString("type");
            try {
                switch (type) {
                    case "create":
                        doCreate(msgHandler, pluginContext, context);
                        break;
                    case "get":
                        doGet(msgHandler, pluginContext, context);
                        break;
                    case "list":
                        doList(msgHandler, pluginContext, context);
                        break;
                    case "update":
                        doUpdate(msgHandler, pluginContext, context);
                        break;
                    case "delete":
                        doDelete(msgHandler, pluginContext, context);
                        break;
                    default:
                        pluginContext.addErrorMessage(context, "Unsupported workshop variable operation type: " + type);
                }
            } catch (Exception e) {
                logger.error("process workshop variable operation '" + type + "' faild", e);
                pluginContext.addErrorMessage(context, "操作失败: " + e.getMessage());
            }
        }

        // ==================================================================
        //  具体操作
        // ==================================================================

        private void doCreate(IControlMsgHandler msgHandler, IPluginContext pluginContext, Context context) throws Exception {
            String ontologyDomainId = requireParam(msgHandler, WorkshopVariable.PARAM_ONTOLOGY_DOMAIN_ID);
            String moduleName = requireParam(msgHandler, WorkshopVariable.PARAM_MODULE_NAME);
            WorkshopVariable variable = parseVariable(msgHandler, context);

            WorkshopVariable.persistVariable(pluginContext, Optional.of(context),
                    ontologyDomainId, moduleName, variable, false);

            pluginContext.setBizResult(context, singleResult(variable));
        }

        private void doGet(IControlMsgHandler msgHandler, IPluginContext pluginContext, Context context) throws Exception {
            String ontologyDomainId = requireParam(msgHandler, WorkshopVariable.PARAM_ONTOLOGY_DOMAIN_ID);
            String moduleName = requireParam(msgHandler, WorkshopVariable.PARAM_MODULE_NAME);
            String variableId = requireParam(msgHandler, WorkshopVariable.PARAM_VARIABLE_ID);

            WorkshopVariable variable = createVariableService().getVariable(ontologyDomainId, moduleName, variableId);
            pluginContext.setBizResult(context, singleResult(variable));
        }

        private void doList(IControlMsgHandler msgHandler, IPluginContext pluginContext, Context context) throws Exception {
            String ontologyDomainId = requireParam(msgHandler, WorkshopVariable.PARAM_ONTOLOGY_DOMAIN_ID);
            String moduleName = requireParam(msgHandler, WorkshopVariable.PARAM_MODULE_NAME);

            List<WorkshopVariable> variables = createVariableService().listVariables(ontologyDomainId, moduleName);
            JSONArray variablesJson = new JSONArray();
            for (WorkshopVariable variable : variables) {
                variablesJson.add(WorkshopVariable.toVariableJSON(variable));
            }

            JSONObject result = new JSONObject();
            result.put(IAjaxResult.KEY_SUCCESS, true);
            result.put("variables", variablesJson);
            result.put("total", variablesJson.size());
            pluginContext.setBizResult(context, result);
        }

        private void doUpdate(IControlMsgHandler msgHandler, IPluginContext pluginContext, Context context) throws Exception {
            String ontologyDomainId = requireParam(msgHandler, WorkshopVariable.PARAM_ONTOLOGY_DOMAIN_ID);
            String moduleName = requireParam(msgHandler, WorkshopVariable.PARAM_MODULE_NAME);
            String variableId = requireParam(msgHandler, WorkshopVariable.PARAM_VARIABLE_ID);

            WorkshopVariable variable = parseVariable(msgHandler, context);
            // 以 URL 参数为准，避免表单里的 id 被改动后写到别的文件上
            variable.setId(variableId);

            WorkshopVariable.persistVariable(pluginContext, Optional.of(context),
                    ontologyDomainId, moduleName, variable, true);

            pluginContext.setBizResult(context, singleResult(variable));
        }

        private void doDelete(IControlMsgHandler msgHandler, IPluginContext pluginContext, Context context) throws Exception {
            String ontologyDomainId = requireParam(msgHandler, WorkshopVariable.PARAM_ONTOLOGY_DOMAIN_ID);
            String moduleName = requireParam(msgHandler, WorkshopVariable.PARAM_MODULE_NAME);
            String variableId = requireParam(msgHandler, WorkshopVariable.PARAM_VARIABLE_ID);

            createVariableService().deleteVariable(pluginContext, Optional.of(context),
                    ontologyDomainId, moduleName, variableId);

            JSONObject result = new JSONObject();
            result.put(IAjaxResult.KEY_SUCCESS, true);
            result.put("message", "Workshop Variable 已删除");
            pluginContext.setBizResult(context, result);
        }

        // ==================================================================
        //  辅助
        // ==================================================================

        /**
         * 变量 CRUD 与模块共用同一个 service 实例，唯一性校验等业务规则得以单点维护
         */
        protected WorkshopModuleService createVariableService() {
            return WorkshopModule.DefaultDescriptor.workshopModuleSvc;
        }

        private static String requireParam(IControlMsgHandler msgHandler, String paramName) {
            String val = msgHandler.getString(paramName);
            if (StringUtils.isEmpty(val)) {
                throw new IllegalStateException("param '" + paramName + "' can not be empty");
            }
            return val;
        }

        /**
         * 解析前端提交的变量实例（自定义端点的 create / update）。
         * <p>
         * {@code {impl, vals}} 正是 {@code AttrValMap.parseDescribableMap()} 期望的输入形态，
         * 由校验/实例化/嵌套子表单（{@code definitionConfig} 的 8 个子类）一并交给框架处理。
         * {@code impl} 是用户选中的变量类型子类（如 {@code StringVariable}），
         * 因此 {@code vals} 里是平铺的字段，不再是多步形态的 {@code multiStepsSavedItems}。
         *
         * @see WorkshopVariable#validateDefinition 「变量类型 ↔ 定义方式」组合合法性的校验点
         */
        private static WorkshopVariable parseVariable(IControlMsgHandler msgHandler, Context context) {
            String variableJson = requireParam(msgHandler, WorkshopVariable.PARAM_VARIABLE);
            AttrValMap attrValMap = AttrValMap.parseDescribableMap(Optional.empty(), JSONObject.parseObject(variableJson));
            return (WorkshopVariable) attrValMap.createDescribable(msgHandler, context).getInstance();
        }

        private static JSONObject singleResult(WorkshopVariable variable) throws Exception {
            JSONObject result = new JSONObject();
            result.put(IAjaxResult.KEY_SUCCESS, true);
            result.put("variable", WorkshopVariable.toVariableJSON(variable));
            return result;
        }
    }
}
