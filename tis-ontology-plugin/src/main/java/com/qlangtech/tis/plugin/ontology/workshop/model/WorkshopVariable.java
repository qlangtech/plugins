package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.MultiStepsSupportHost;
import com.qlangtech.tis.extension.MultiStepsSupportHostDescriptor;
import com.qlangtech.tis.extension.OneStepOfMultiSteps;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.manage.common.IAjaxResult;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.WorkshopModule;
import com.qlangtech.tis.plugin.ontology.workshop.enums.VariableType;
import com.qlangtech.tis.plugin.ontology.workshop.model.definition.VariableDefinitionConfig;
import com.qlangtech.tis.plugin.ontology.workshop.service.WorkshopModuleService;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.util.AttrValMap;
import com.qlangtech.tis.util.DefaultDescriptorsJSON;
import com.qlangtech.tis.util.DescribableJSON;
import com.qlangtech.tis.util.IPluginContext;
import com.qlangtech.tis.util.UploadPluginMeta;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Workshop Variable 实体 —— Workshop 的模块级变量定义，也是整个 Workshop 的数据流引擎。
 *
 * <p>页面、浮层、组件之间不直接通信，全部通过读/写变量传递数据。设计公式：
 * <pre>
 *   Variable = Type + Definition + Value + Dependencies
 * </pre>
 * 设计文档：/opt/misc/palantir-study/use-case-data-quality/detail-design/workshop/03-variable-system.md
 *
 * <p>实体层次上，它与 pages / overlays / header 并列，是 {@code WorkshopModule} 聚合根下的子实体之一。
 *
 * <h3>为什么是多步插件</h3>
 * 变量类型（{@link MetadataOfVariable#type}）与定义方式（{@link DefinitionOfVariable#definitionConfig}）
 * 并非彼此独立：只有一部分定义方式能产出某一类值（「对象集合定义」只能产出 {@code OBJECT_SET}，
 * 「函数计算」能产出绝大多数类型），两者是 12×7 的多对多关系，映射表见
 * {@link VariableDefinitionConfig#TYPE_DEFINITIONS}。
 * 挤在一张表单里时，用户会同时看到两个互不相关的控件，能选出「数值类型的对象集合定义」这类
 * 自相矛盾的状态。拆成两步后，第二步的候选集由第一步的选择收敛
 * （{@link DefinitionOfVariable#descFilter}）。本类因此成为<b>多步宿主</b>：
 * 自身没有任何可渲染属性（{@code MultiStepsHostPluginFormProperties} 的
 * {@code getKVTuples()} 恒为空），字段全部装在 {@link #stepsPlugin} 里。
 *
 * <h3>两步的内容</h3>
 * <ol>
 *   <li>{@link MetadataOfVariable} —— 元数据：{@code id}（落盘文件名）、{@code name}
 *       （模块内唯一引用名，widget 通过它反向引用变量）、{@code type}（值的数据形态，
 *       见 {@link VariableType}）</li>
 *   <li>{@link DefinitionOfVariable} —— 值怎么算（{@code definitionConfig}，
 *       具体子类的 Java 类型<em>即是</em>「定义类型」本身，因此没有平行的类型字段）
 *       与运行时行为（{@code recomputeBehavior} / {@code lazyLoading} / {@code interfaceConfig} /
 *       {@code routingConfig} / {@code stateSavingConfig}）</li>
 * </ol>
 * 前后端分工由 {@link VariableDefinitionConfig#isBackendComputed()} 表达。
 *
 * <h3>类型身份与落盘校验</h3>
 * 「值怎么算」由 {@link DefinitionOfVariable#definitionConfig} 的具体子类承担，<b>没有</b>与之
 * 平行的类型枚举或类型字段 —— 详见 {@link VariableDefinitionConfig} 的类注释（原先的
 * {@code definitionType} 字段与 {@code VariableDefinitionType} 枚举因属于重复分类体系已被删除）。
 * {@link DefinitionOfVariable#descFilter} 的下拉过滤只是前端体验，落盘守卫是
 * {@link DefaultDescriptor#validateDefinition} —— 绕过前端直接 POST 非法组合一样会被拒绝。
 *
 * <h3>消费方</h3>
 * <ol>
 *   <li><b>配置期</b>：WidgetOptionHelper 把变量名列表作为 widget 表单的 SELECTABLE 选项
 *       （objectSetVar / textVariable），并据选中的变量反查其引用的 Ontology 对象类型属性列表，
 *       供 labelProp / startProp / endProp 等级联字段使用 —— 变量在此充当
 *       「widget 配置 → Ontology 元数据」的桥梁。为此本类提供了
 *       {@link #getName()} / {@link #getType()} / {@link #getDefinitionConfig()} 等便捷访问器，
 *       消费方不必自己下钻 {@link #getMeta()} / {@link #getDefinition()}</li>
 *   <li><b>运行期</b>：前端 VariableService 构建依赖 DAG、拓扑排序、变更传播自动重算</li>
 *   <li><b>运行期</b>：{@code VariableBasedVisibility}（Overlay 用，支持比较表达式）与
 *       {@code ConditionalVisibility}（Section / Widget 用，仅判真假）引用变量做条件可见性</li>
 *   <li><b>运行期</b>：routingConfig 驱动跳转，stateSavingConfig 持久化变量值</li>
 * </ol>
 *
 * <h3>持久化</h3>
 * {@code WorkshopModule.variables} 是 {@code transient} 字段（子实体与聚合根分开落盘），
 * XStream 序列化插件配置时会跳过它，所以变量不随模块 XML 落盘，而是与 pages 平行地存放在
 * 各自的独立文件里：{@code ontology/{domain}/workshop_variables/{moduleName}/{variableId}.xml}
 * （见 {@link com.qlangtech.tis.plugin.ontology.workshop.store.WorkshopVariableStore}）。
 * {@link #stepsPlugin} 则是普通实例字段，随变量自己的 XML 一起落盘。
 * 模块下的变量列表由 {@link WorkshopModule#load(String, String)} 在加载模块时回填到
 * {@code variables} 字段，供 {@link com.qlangtech.tis.plugin.ontology.workshop.widget.impl.WidgetOptionHelper}
 * 之类的消费方使用。
 *
 * <h3>变量自己的 HTTP 路由</h3>
 * CRUD 由 {@link DefaultDescriptor#httpProcess(IControlMsgHandler, IPluginContext, Context)} 承担，
 * 而不是塞进 {@code WorkshopModule.DefaultDescriptor} 的 switch：
 * {@code PluginAction.doDescriptionProcess()} 是按 {@code impl} 参数解析 descriptor 的，
 * 而 {@code Descriptor.getId()} 返回的是 Describable 的 FQCN，因此
 * {@code impl=com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopVariable} 能直接寻址到这里。
 * 变量随后由 {@link WorkshopModuleService} 落到 {@link com.qlangtech.tis.plugin.ontology.workshop.store.WorkshopVariableStore}。
 * <p>
 * 该端点的请求体形如
 * {@code {impl, vals:{multiStepsSavedItems:[{step1},{step2}]}}}，
 * {@link AttrValMap#parseDescribableMap} 会走框架的
 * {@code Descriptor#parseDescribable} → {@code visitMultiStepsHost} 分支完成 {@link #setSteps}，
 * 因此多步改造没有给这条自定义链路带来额外适配。
 *
 * @see MetadataOfVariable
 * @see DefinitionOfVariable
 */
public class WorkshopVariable implements Describable<WorkshopVariable>, Serializable, IdentityName,
        MultiStepsSupportHost, IPluginStore.ManipuldateProcessor {

    private static final Logger logger = LoggerFactory.getLogger(WorkshopVariable.class);

    /**
     * 变量 CRUD 的参数名，均为显式传入 —— 变量的 Descriptor 不是
     * {@code OntologyDomainManipulate.BasicDesc} 的子类，没有可从插件元数据推断 domain 的上下文。
     */
    static final String PARAM_ONTOLOGY_DOMAIN_ID = "ontologyDomainId";
    static final String PARAM_MODULE_NAME = "moduleName";
    static final String PARAM_VARIABLE_ID = "variableId";
    /**
     * 变量实例本体，{@code {impl, vals}} 结构的 JSON 字符串（即前端 {@code Item.project()} 的输出）
     */
    static final String PARAM_VARIABLE = "variable";

    /**
     * 步骤数固定为 2（元数据 + 定义），见类注释。
     */
    private static final int FIXED_VARIABLE_STEPS_LENGTH = 2;

    /**
     * Caution: 这个字段目前没有用，由于实现了 {@link IdentityName} 接口 ——
     * 框架要求 IdentityName 的实现类有且仅有一个 {@code identity = true} 字段，
     * 而变量的真正主键是第一步 {@link MetadataOfVariable#id}，由 {@link #identityValue()} 取回。
     *
     * @see com.qlangtech.tis.plugin.ontology.OntologyValueType#useless
     */
    @FormField(identity = true, ordinal = 0, validate = {Validator.require, Validator.identity})
    public String useless;

    /**
     * 两步的配置实例，索引即步骤序号（{@code Step1.getStepIndex() == 0}）。
     * 随变量自己的 XML 落盘，因此这个字段<b>不能</b>是 transient。
     */
    protected OneStepOfMultiSteps[] stepsPlugin;

    public WorkshopVariable() {
    }

    @Override
    public void setSteps(OneStepOfMultiSteps[] stepsPlugin) {
        this.stepsPlugin = Objects.requireNonNull(stepsPlugin, "stepsPlugin can not be null");
        if (stepsPlugin.length != FIXED_VARIABLE_STEPS_LENGTH) {
            throw new IllegalStateException("stepsPlugin.length must be equal to " + FIXED_VARIABLE_STEPS_LENGTH);
        }
    }

    /**
     * 注意：序列化注解由 {@link MultiStepsSupportHost} 接口上的
     * {@code @JSONField(serialize = true, name = KEY_MULTI_STEPS_SAVED_ITEMS)} 提供，
     * 落到 JSON 上的键就是 {@code multiStepsSavedItems}。
     */
    @Override
    public OneStepOfMultiSteps[] getMultiStepsSavedItems() {
        return stepsPlugin;
    }

    @Override
    public String identityValue() {
        return getMeta().id;
    }

    // ==================================================================
    //  步骤访问
    // ==================================================================

    /**
     * 第一步：变量的元数据（id / name / type）
     */
    public MetadataOfVariable getMeta() {
        return (MetadataOfVariable) Objects.requireNonNull(
                stepsPlugin[OneStepOfMultiSteps.Step.Step1.getStepIndex()], "step1 can not be null");
    }

    /**
     * 第二步：值的计算方式与运行时行为
     */
    public DefinitionOfVariable getDefinition() {
        return (DefinitionOfVariable) Objects.requireNonNull(
                stepsPlugin[OneStepOfMultiSteps.Step.Step2.getStepIndex()], "step2 can not be null");
    }

    /**
     * 唯一标识，决定变量的落盘文件名
     */
    public String getId() {
        return getMeta().id;
    }

    public void setId(String id) {
        getMeta().id = id;
    }

    /**
     * 模块内唯一引用名（大小写不敏感）
     */
    public String getName() {
        return getMeta().name;
    }

    /**
     * 值的数据形态
     */
    public VariableType getType() {
        return getMeta().type;
    }

    /**
     * 值怎么算，具体子类的 Java 类型即是「定义类型」本身
     */
    public VariableDefinitionConfig getDefinitionConfig() {
        return getDefinition().definitionConfig;
    }

    // ==================================================================
    //  持久化
    // ==================================================================

    /**
     * 标准 {@code plugin_action} 链路（{@link IPluginStore#noSaveStore}）的落盘入口 ——
     * 前端 {@code PluginsComponent.openPluginDialog()} 打开的多步配置向导提交时就走到这里。
     *
     * <p>存储位置由插件元数据的 extraParam 显式传入（{@code ontologyDomainId} / {@code moduleName}）：
     * 本 Descriptor 不是 {@code OntologyDomainManipulate.BasicDesc} 的子类，
     * 没有可从元数据推断 domain 的上下文。是新建还是更新由 {@code pluginMeta.isUpdate()}
     * （即 extraParam {@code update_true}）决定，两者最终都路由到 {@link #persistVariable}，
     * 与变量自己的自定义端点共用同一份业务规则（模块内变量名唯一等），避免两份实现漂移。
     *
     * @see DefaultDescriptor#httpProcess(IControlMsgHandler, IPluginContext, Context)
     *      变量自己的端点，list / delete 仍走那条路
     */
    @Override
    public void manipuldateProcess(IPluginContext pluginContext, UploadPluginMeta pluginMeta,
                                   Optional<Context> context) {
        String ontologyDomainId = pluginMeta.getExtraParam(PARAM_ONTOLOGY_DOMAIN_ID);
        String moduleName = pluginMeta.getExtraParam(PARAM_MODULE_NAME);
        if (StringUtils.isEmpty(ontologyDomainId) || StringUtils.isEmpty(moduleName)) {
            throw new IllegalStateException("plugin meta extra param '" + PARAM_ONTOLOGY_DOMAIN_ID + "' and '"
                    + PARAM_MODULE_NAME + "' must not be empty");
        }
        persistVariable(pluginContext, context, ontologyDomainId, moduleName, this, pluginMeta.isUpdate());
    }

    /**
     * 变量落盘，供 {@link DefaultDescriptor#doCreate} / {@link DefaultDescriptor#doUpdate} /
     * {@link #manipuldateProcess} 共用。
     *
     * <p>「变量类型 ↔ 定义方式」的落盘守卫也收敛在此：标准链路不经过
     * {@link DefaultDescriptor#parseVariable}，校验放在这里两条路才都绕不过去。
     */
    private static void persistVariable(IPluginContext pluginContext, Optional<Context> context,
                                        String ontologyDomainId, String moduleName,
                                        WorkshopVariable variable, boolean update) {
        DefaultDescriptor.validateDefinition(variable);
        WorkshopModuleService service = WorkshopModule.DefaultDescriptor.workshopModuleSvc;
        if (update) {
            service.updateVariable(pluginContext, context, ontologyDomainId, moduleName, variable);
        } else {
            service.addVariable(pluginContext, context, ontologyDomainId, moduleName, variable);
        }
    }

    @TISExtension
    public static class DefaultDescriptor extends Descriptor<WorkshopVariable>
            implements MultiStepsSupportHostDescriptor<WorkshopVariable> {

        @Override
        public String getDisplayName() {
            return "Workshop Variable";
        }

        @Override
        public Class<WorkshopVariable> getHostClass() {
            return WorkshopVariable.class;
        }

        @Override
        public List<OneStepOfMultiSteps.BasicDesc> getStepDescriptionList() {
            // 顺序即步骤顺序：第一个元素是第一步
            return List.of(new MetadataOfVariable.Desc(), new DefinitionOfVariable.Desc());
        }

        /**
         * 把两步的 descriptor 元数据预埋进前端的多步执行上下文。
         *
         * <p>标准多步链路里，下一步的元数据是服务端往返给的
         * （{@code OneStepOfMultiSteps#manipuldateProcess} 产出的 {@code nextStepPluginDesc}）。
         * 本插件走的是自定义 {@code httpProcess} 端点，没有那次往返，所以在这里一次性把
         * <b>每一步</b>的 descriptor 都下发，前端可离线切换到任意一步。
         *
         * <p>产物落在 {@code DescriptorsJSON.visitMultiStepsHost()} 的
         * {@code multiStepsCfg.context} 里，前端 {@code MultiStepsDescriptor.stepExecContext} 即由它而来。
         *
         * <p>注意这里生成第二步元数据时<b>没有第一步的上下文</b>
         * （{@code MetadataOfVariable} 实例尚不存在），所以
         * {@link DefinitionOfVariable#descFilter} 会走「全量返回」分支 ——
         * 这是刻意的：真正的收敛发生在落盘校验（{@link #validateDefinition}），
         * 而前端拿到第一步的值后可以调同一个入口自行过滤。
         */
        @Override
        public void appendExternalProps(JSONObject stepContext) {
            JSONArray stepDescList = new JSONArray();
            int stepIndex = 0;
            for (OneStepOfMultiSteps.BasicDesc stepDesc : getStepDescriptionList()) {
                JSONObject oneStep = new JSONObject();
                oneStep.put("stepIndex", stepIndex++);
                oneStep.put("stepName", stepDesc.getDisplayName());
                oneStep.put("stepDescription", stepDesc.getStepDescription());
                oneStep.put("descriptor", new DefaultDescriptorsJSON(stepDesc).getDescriptorsJSON());
                stepDescList.add(oneStep);
            }
            stepContext.put("stepDescList", stepDescList);
        }

        /**
         * 按 type 参数分派变量的增删改查。
         *
         * <pre>
         * POST /coredefine/corenodemanage.ajax
         *   ?event_submit_do_description_process=y
         *   &amp;action=plugin_action
         *   &amp;impl=com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopVariable
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
            String ontologyDomainId = requireParam(msgHandler, PARAM_ONTOLOGY_DOMAIN_ID);
            String moduleName = requireParam(msgHandler, PARAM_MODULE_NAME);
            WorkshopVariable variable = parseVariable(msgHandler, context);

            persistVariable(pluginContext, Optional.of(context), ontologyDomainId, moduleName, variable, false);

            pluginContext.setBizResult(context, singleResult(variable));
        }

        private void doGet(IControlMsgHandler msgHandler, IPluginContext pluginContext, Context context) throws Exception {
            String ontologyDomainId = requireParam(msgHandler, PARAM_ONTOLOGY_DOMAIN_ID);
            String moduleName = requireParam(msgHandler, PARAM_MODULE_NAME);
            String variableId = requireParam(msgHandler, PARAM_VARIABLE_ID);

            WorkshopVariable variable = createVariableService().getVariable(ontologyDomainId, moduleName, variableId);
            pluginContext.setBizResult(context, singleResult(variable));
        }

        private void doList(IControlMsgHandler msgHandler, IPluginContext pluginContext, Context context) throws Exception {
            String ontologyDomainId = requireParam(msgHandler, PARAM_ONTOLOGY_DOMAIN_ID);
            String moduleName = requireParam(msgHandler, PARAM_MODULE_NAME);

            List<WorkshopVariable> variables = createVariableService().listVariables(ontologyDomainId, moduleName);
            JSONArray variablesJson = new JSONArray();
            for (WorkshopVariable variable : variables) {
                variablesJson.add(toVariableJSON(variable));
            }

            JSONObject result = new JSONObject();
            result.put(IAjaxResult.KEY_SUCCESS, true);
            result.put("variables", variablesJson);
            result.put("total", variablesJson.size());
            pluginContext.setBizResult(context, result);
        }

        private void doUpdate(IControlMsgHandler msgHandler, IPluginContext pluginContext, Context context) throws Exception {
            String ontologyDomainId = requireParam(msgHandler, PARAM_ONTOLOGY_DOMAIN_ID);
            String moduleName = requireParam(msgHandler, PARAM_MODULE_NAME);
            String variableId = requireParam(msgHandler, PARAM_VARIABLE_ID);

            WorkshopVariable variable = parseVariable(msgHandler, context);
            // 以 URL 参数为准，避免表单里的 id 被改动后写到别的文件上
            variable.setId(variableId);

            persistVariable(pluginContext, Optional.of(context), ontologyDomainId, moduleName, variable, true);

            pluginContext.setBizResult(context, singleResult(variable));
        }

        private void doDelete(IControlMsgHandler msgHandler, IPluginContext pluginContext, Context context) throws Exception {
            String ontologyDomainId = requireParam(msgHandler, PARAM_ONTOLOGY_DOMAIN_ID);
            String moduleName = requireParam(msgHandler, PARAM_MODULE_NAME);
            String variableId = requireParam(msgHandler, PARAM_VARIABLE_ID);

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
         * 由校验/实例化/嵌套子表单（{@code definitionConfig} 的 7 个子类）一并交给框架处理。
         * 因为 {@link WorkshopVariable} 是多步宿主，{@code vals} 里是多步形态的
         * {@code multiStepsSavedItems}（或 {@code Step1}/{@code Step2}），
         * 框架的 {@code visitMultiStepsHost} 分支会完成两步的实例化与 {@code setSteps}。
         *
         * @see WorkshopVariable#persistVariable 「变量类型 ↔ 定义方式」组合合法性的校验点
         */
        private static WorkshopVariable parseVariable(IControlMsgHandler msgHandler, Context context) {
            String variableJson = requireParam(msgHandler, PARAM_VARIABLE);
            AttrValMap attrValMap = AttrValMap.parseDescribableMap(Optional.empty(), JSONObject.parseObject(variableJson));
            return (WorkshopVariable) attrValMap.createDescribable(msgHandler, context).getInstance();
        }

        /**
         * 校验 (type, definitionConfig) 组合落在
         * {@link VariableDefinitionConfig#TYPE_DEFINITIONS} 允许的范围内。
         *
         * <p>表单侧的 {@code subDescEnumFilter}（接线见 {@code DefinitionOfVariable.json}）已经把
         * 非法组合从下拉里滤掉了，但那只是体验：绕过前端直接 POST 仍能造出「数值类型的对象集合」
         * 这类自相矛盾的状态——这正是当初删掉 {@code definitionType} 字段想避免的一类问题。
         * 因此落盘前在此兜底，由 {@link WorkshopVariable#persistVariable} 在自定义端点与
         * 标准 {@code save_plugin_config} 两条链路上统一调用。
         *
         * @see VariableDefinitionConfig#supports(VariableType, com.qlangtech.tis.extension.Descriptor)
         */
        private static void validateDefinition(WorkshopVariable variable) {
            VariableType type = variable.getType();
            VariableDefinitionConfig definitionConfig = variable.getDefinitionConfig();
            if (type == null || definitionConfig == null) {
                // 缺值由 @FormField(validate = {Validator.require}) 在表单侧拦截，这里不重复报错
                return;
            }
            if (!VariableDefinitionConfig.supports(type, definitionConfig.getDescriptor())) {
                throw new IllegalStateException("变量类型 " + type + " 不能由 "
                        + definitionName(definitionConfig.getClass().getName()) + " 定义，可用的是："
                        + supportedDefinitionNames(type));
            }
        }

        /**
         * 列出某变量类型允许的全部定义方式。
         * <p>
         * 只用 descriptor 的 {@code Class} 推导名字、不实例化 descriptor：构造 descriptor
         * 可能有副作用（如注册 select options），只为拼一条报错信息不值得。
         */
        private static String supportedDefinitionNames(VariableType type) {
            return VariableDefinitionConfig.definitionsOf(type).stream() //
                    .map(descClazz -> definitionName(descClazz.getName())) //
                    .sorted() //
                    .collect(Collectors.joining(", "));
        }

        /**
         * {@code com.qlangtech...definition.StaticConfig$DefaultDescriptor} → {@code StaticConfig}
         */
        private static String definitionName(String descClazzName) {
            int nested = descClazzName.indexOf('$');
            String outer = nested > -1 ? descClazzName.substring(0, nested) : descClazzName;
            return outer.substring(outer.lastIndexOf('.') + 1);
        }

        private static JSONObject singleResult(WorkshopVariable variable) throws Exception {
            JSONObject result = new JSONObject();
            result.put(IAjaxResult.KEY_SUCCESS, true);
            result.put("variable", toVariableJSON(variable));
            return result;
        }

        /**
         * 变量 → 前端可直接回填的表单结构。
         * <p>
         * {@code DescribableJSON.getItemJson()} 产出 {@code {impl, displayName, vals}}：
         * {@code impl} 是本 descriptor 的 id（即 {@link WorkshopVariable} 的 FQCN）。
         * 因为宿主是多步插件，{@code vals} 形如
         * {@code {allStepDesc, multiStepsSavedItems:[{impl:MetadataOfVariable...},
         * {impl:DefinitionOfVariable...}]}}，其中嵌套的 {@code definitionConfig} 以自己的
         * {@code impl} 表达具体子类（见
         * {@code MultiStepsHostPluginFormProperties#getInstancePropsJson}）。
         * <p>
         * 模块侧下发变量列表时复用本方法，保证两处 JSON 形态一致。
         */
        public static JSONObject toVariableJSON(WorkshopVariable variable) throws Exception {
            return new DescribableJSON<>(variable).getItemJson();
        }
    }
}
