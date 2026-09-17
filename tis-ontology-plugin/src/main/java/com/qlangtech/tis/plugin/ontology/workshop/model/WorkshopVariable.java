package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.util.PluginExtraProps;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.WorkshopModule;
import com.qlangtech.tis.plugin.ontology.workshop.enums.RecomputeBehavior;
import com.qlangtech.tis.plugin.ontology.workshop.enums.VariableType;
import com.qlangtech.tis.plugin.ontology.workshop.model.config.MappingInterfaceConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.config.NoneInterfaceConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.config.VariableInterfaceConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.config.VariableRoutingConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.config.VariableStateSavingConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.definition.VariableDefinitionConfig;
import com.qlangtech.tis.plugin.ontology.workshop.service.WorkshopModuleService;
import com.qlangtech.tis.util.DescribableJSON;
import com.qlangtech.tis.util.IPluginContext;
import com.qlangtech.tis.util.UploadPluginMeta;
import org.apache.commons.lang.StringUtils;

import java.io.Serializable;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
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
 * <h3>类型由子类承担，因此是单步插件</h3>
 * 变量类型（{@link VariableType}）与定义方式（{@link #definitionConfig}）并非彼此独立：
 * 只有一部分定义方式能产出某一类值（「对象集合定义」只能产出 {@code OBJECT_SET}，
 * 「函数计算」能产出绝大多数类型），两者是 12×8 的多对多关系，映射表见
 * {@link VariableDefinitionConfig#TYPE_DEFINITIONS}。挤在一张表单里时，用户会同时看到两个
 * 互不相关的控件，能选出「数值类型的对象集合定义」这类自相矛盾的状态。
 *
 * <p>本类此前是 {@code MultiStepsSupportHost}（第一步选类型、第二步选定义方式），靠第二步
 * 读取第一步实例的<em>运行时上下文</em>来收敛候选集。现在改为：<b>每个 {@link VariableType}
 * 常量对应一个本类的具体子类</b>（见 {@code ...workshop.model.variable} 包），类型成了
 * <em>编译期的静态事实</em>，收敛条件因此可以写死在子类的 descriptor 里
 * （见 {@link BasicDescriptor}），两步的全部动机随之消失 —— 与
 * {@link VariableDefinitionConfig} 删掉平行的 {@code definitionType} 字段是同一条原则：
 * 枚举<em>作为实例字段</em>是重复的分类体系，枚举<em>作为静态类型表</em>不是。
 *
 * <p>子类可以增补自己的 {@code @FormField}（自有字段的 ordinal 从 <b>100</b> 起，
 * 避免与基类的 -1..6 撞号导致排序不确定）；只有声明了字段的子类才需要自己的 {@code .json}。
 *
 * <h3>落盘校验</h3>
 * 表单侧的下拉过滤只是体验，权威的落盘守卫是 {@link #validateDefinition(WorkshopVariable)} ——
 * 绕过前端直接 POST 非法组合一样会被拒绝。
 *
 * <h3>消费方</h3>
 * <ol>
 *   <li><b>配置期</b>：{@code WidgetOptionHelper} 把变量名列表作为 widget 表单的 SELECTABLE 选项
 *       （objectSetVar / textVariable），并据选中的变量反查其引用的 Ontology 对象类型属性列表，
 *       供 labelProp / startProp / endProp 等级联字段使用 —— 变量在此充当
 *       「widget 配置 → Ontology 元数据」的桥梁。为此本类提供了
 *       {@link #getName()} / {@link #getVariableType()} / {@link #getDefinitionConfig()} 便捷访问器</li>
 *   <li><b>运行期</b>：前端 VariableService 构建依赖 DAG、拓扑排序、变更传播自动重算</li>
 *   <li><b>运行期</b>：{@code VariableBasedVisibility}（Overlay 用，支持比较表达式）与
 *       {@code ConditionalVisibility}（Section / Widget 用，仅判真假）引用变量做条件可见性</li>
 *   <li><b>运行期</b>：routingConfig 驱动跳转，stateSavingConfig 持久化变量值</li>
 * </ol>
 *
 * <h3>持久化</h3>
 * {@code WorkshopModule.variables} 是 {@code transient} 字段（子实体与聚合根分开落盘），
 * XStream 序列化插件配置时会跳过它，所以变量不随模块 XML 落盘，而是与 pages 平行地存放在
 * 各自的独立文件里：{@code ontology/{domain}/workshop_variables/{moduleName}/{id}.xml}
 * （见 {@link com.qlangtech.tis.plugin.ontology.workshop.store.WorkshopVariableStore}）。
 * 文件里记录的是<b>具体子类</b>的类名，因此重命名或移动子类会让旧文件被静默跳过。
 *
 * <h3>变量自己的 HTTP 路由</h3>
 * CRUD 由 {@link com.qlangtech.tis.plugin.ontology.workshop.desc.WorkshopVariableOperationDesc}
 * 承担。基类抽象之后不再有以 {@code WorkshopVariable} 为 {@code clazz} 的 descriptor
 * （{@code Descriptor.getId()} 返回的正是 {@code clazz} 的 FQCN），因此端点不能挂在本类的
 * descriptor 上，改为独立的一个描述符类，前端用它的 FQCN 作 {@code impl} 参数寻址。
 *
 * @see com.qlangtech.tis.plugin.ontology.workshop.model.variable.StringVariable
 * @see BasicDescriptor
 */
public abstract class WorkshopVariable implements Describable<WorkshopVariable>, Serializable, IdentityName,
        IPluginStore.ManipuldateProcessor {

    /**
     * 变量 CRUD 的参数名，均为显式传入 —— 变量的 Descriptor 不是
     * {@code OntologyDomainManipulate.BasicDesc} 的子类，没有可从插件元数据推断 domain 的上下文。
     */
    public static final String PARAM_ONTOLOGY_DOMAIN_ID = "ontologyDomainId";
    public static final String PARAM_MODULE_NAME = "moduleName";
    public static final String PARAM_VARIABLE_ID = "variableId";
    /**
     * 变量实例本体，{@code {impl, vals}} 结构的 JSON 字符串（即前端 {@code Item.project()} 的输出）
     */
    public static final String PARAM_VARIABLE = "variable";

    public static final String KEY_ID = "id";
    public static final String KEY_NAME = "name";
    public static final String KEY_DEFINITION_CONFIG = "definitionConfig";
    /**
     * 变量类型在实例 JSON <b>顶层</b>的键名（枚举常量名，如 {@code OBJECT_SET_FILTER}）。
     *
     * @see #toVariableJSON(WorkshopVariable)
     */
    public static final String KEY_VARIABLE_TYPE = "variableType";

    /**
     * 唯一标识，决定变量的落盘文件名
     * （{@code ontology/{domain}/workshop_variables/{moduleName}/{id}.xml}）。
     *
     * <p>构造时生成 UUID，新建后由前端回传、或由后端从 URL 参数覆盖（见
     * {@code WorkshopVariableOperationDesc#doUpdate}）。
     * {@code ordinal = -1} 使其不参与表单排序（前端不渲染可编辑控件）。
     *
     * <p>这是 {@link IdentityName} 要求的<em>唯一</em>一个 {@code identity = true} 字段 ——
     * 框架的 {@code Descriptor.getPropertyTypes()} 会校验数量，多一个少一个都直接抛异常。
     */
    @FormField(ordinal = -1, identity = true, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String id;

    public static String dftValOfId() {
        return UUID.randomUUID().toString();
    }

    /**
     * 模块内唯一引用名（大小写不敏感），widget 通过它反向引用变量。
     * 唯一性由 {@code WorkshopModuleService} 在落盘前校验。
     */
    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
    public String name;

    /**
     * 值怎么算，见 {@link VariableDefinitionConfig}。
     * 具体子类的 Java 类型<em>即是</em>「定义类型」本身，因此没有平行的类型字段。
     *
     * <p>本字段的候选 impl 由 {@link BasicDescriptor} 按子类代表的 {@link VariableType} 收敛。
     */
    @FormField(ordinal = 1, validate = {Validator.require})
    public VariableDefinitionConfig definitionConfig;

    /**
     * 依赖传播时是否自动重算，仅 AUTOMATIC 会随上游变更自动传播
     */
    @FormField(ordinal = 2, type = FormFieldType.ENUM)
    public RecomputeBehavior recomputeBehavior = RecomputeBehavior.AUTOMATIC;

    /**
     * 是否延迟加载变量值，直到实际使用时才计算
     */
    @FormField(ordinal = 3, type = FormFieldType.ENUM, validate = {Validator.require})
    public Boolean lazyLoading = false;

    /**
     * 变量与外部接口的输入映射（模块间变量传递）。
     *
     * <p>「开 / 关」由<b>子类类型</b>表达（{@link MappingInterfaceConfig} /
     * {@link NoneInterfaceConfig}），不是实例上的布尔字段。因此这里
     * <b>不能</b>给 {@code @FormField} 指定 {@code type}：字段类型是
     * {@code Describable} 时框架自动生成 impl 选择器，指定 {@code SELECTABLE}
     * 会在运行期报错。默认项由 {@code WorkshopVariable.json} 的
     * {@code dftVal: "off"} 决定 —— 它按 {@code displayName} 匹配子类，
     * 故「关闭」子类的 {@code getDisplayName()} 必须返回 {@code Descriptor.SWITCH_OFF}。
     */
    @FormField(ordinal = 4)
    public VariableInterfaceConfig interfaceConfig;

    /**
     * 变量值变化后的页面跳转行为。开关机制同 {@link #interfaceConfig}。
     */
    @FormField(ordinal = 5)
    public VariableRoutingConfig routingConfig;

    /**
     * 变量值的持久化策略。开关机制同 {@link #interfaceConfig}。
     */
    @FormField(ordinal = 6)
    public VariableStateSavingConfig stateSavingConfig;


    /**
     * 本实例代表的变量类型 —— 由子类固化为常量，取代原先的 {@code type} 字段。
     *
     * <p>这是本类与 {@code ...workshop.model.variable} 包下 12 个子类之间唯一的约定，
     * 同时也是 {@link BasicDescriptor} 注入 {@code definitionConfig} 候选过滤脚本的依据。
     */
    public abstract VariableType getVariableType();

    @Override
    public String identityValue() {
        return getId();
    }

    /**
     * 唯一标识，决定变量的落盘文件名
     */
    public String getId() {
        return this.id;
    }

    public void setId(String id) {
        this.id = id;
    }

    /**
     * 模块内唯一引用名（大小写不敏感）
     */
    public String getName() {
        return this.name;
    }

    /**
     * 值怎么算，具体子类的 Java 类型即是「定义类型」本身
     */
    public VariableDefinitionConfig getDefinitionConfig() {
        return this.definitionConfig;
    }

    // ==================================================================
    //  序列化
    // ==================================================================

    /**
     * 变量 → 前端可直接回填的表单结构。
     *
     * <p>{@code DescribableJSON.getItemJson()} 产出 {@code {impl, displayName, vals}}：
     * {@code impl} 是变量<b>具体子类</b>的 FQCN（如 {@code StringVariable}），前端据此取回
     * 对应子类的 descriptor 来打开编辑表单；{@code vals} 是平铺的字段，其中嵌套的
     * {@code definitionConfig} 以自己的 {@code impl} 表达具体子类。
     *
     * <p>另外在<b>顶层</b>补一个 {@link #KEY_VARIABLE_TYPE}，供前端判断变量类型而不必自己
     * 从 FQCN 反推。刻意不放进 {@code vals}：那是要回填成表单字段的结构，
     * 多一个并非字段的键会被当成未知表单项。
     *
     * <p>模块侧下发变量列表（{@code WorkshopModule} 的 module payload）与
     * {@code WorkshopVariableOperation} 的 CRUD 端点共用本方法，保证两处 JSON 形态一致。
     */
    public static JSONObject toVariableJSON(WorkshopVariable variable) throws Exception {
        JSONObject itemJson = new DescribableJSON<>(variable).getItemJson();
        VariableType variableType = variable.getVariableType();
        itemJson.put(KEY_VARIABLE_TYPE, variableType == null ? null : variableType.name());
        return itemJson;
    }

    // ==================================================================
    //  持久化
    // ==================================================================

    /**
     * 标准 {@code plugin_action} 链路（{@link IPluginStore#noSaveStore}）的落盘入口 ——
     * 前端 {@code PluginsComponent.openPluginDialog()} 打开的单步配置表单提交时就走到这里。
     *
     * <p>存储位置由插件元数据的 extraParam 显式传入（{@code ontologyDomainId} / {@code moduleName}）：
     * 本类不是 {@code OntologyDomainManipulate.BasicDesc} 的子类，
     * 没有可从元数据推断 domain 的上下文。是新建还是更新由 {@code pluginMeta.isUpdate()}
     * （即 extraParam {@code update_true}）决定，两者最终都路由到 {@link #persistVariable}，
     * 与变量自己的自定义端点共用同一份业务规则（模块内变量名唯一等），避免两份实现漂移。
     *
     * @see com.qlangtech.tis.plugin.ontology.workshop.desc.WorkshopVariableOperationDesc
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
     * 变量落盘，供 {@code WorkshopVariableOperationDesc} 的 create / update 与
     * {@link #manipuldateProcess} 共用。
     *
     * <p>「变量类型 ↔ 定义方式」的落盘守卫也收敛在此：标准链路不经过
     * {@code WorkshopVariableOperationDesc#parseVariable}，校验放在这里两条路才都绕不过去。
     */
    public static void persistVariable(IPluginContext pluginContext, Optional<Context> context,
                                       String ontologyDomainId, String moduleName,
                                       WorkshopVariable variable, boolean update) {
        validateDefinition(variable);
        WorkshopModuleService service = WorkshopModule.DefaultDescriptor.workshopModuleSvc;
        if (update) {
            service.updateVariable(pluginContext, context, ontologyDomainId, moduleName, variable);
        } else {
            service.addVariable(pluginContext, context, ontologyDomainId, moduleName, variable);
        }
    }

    /**
     * 校验 (type, definitionConfig) 组合落在
     * {@link VariableDefinitionConfig#TYPE_DEFINITIONS} 允许的范围内。
     *
     * <p>表单侧的 {@code subDescEnumFilter} 已经把非法组合从下拉里滤掉了，但那只是体验：
     * 绕过前端直接 POST 仍能造出「数值类型的对象集合」这类自相矛盾的状态——这正是当初删掉
     * {@code definitionType} 字段想避免的一类问题。因此落盘前在此兜底。
     *
     * @see VariableDefinitionConfig#supports(VariableType, com.qlangtech.tis.extension.Descriptor)
     */
    public static void validateDefinition(WorkshopVariable variable) {
        VariableType type = variable.getVariableType();
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

    // ==================================================================
    //  描述符基类
    // ==================================================================

    /**
     * 12 个变量类型子类共享的 descriptor 基类。
     *
     * <h3>它解决的唯一问题：definitionConfig 的候选集收敛</h3>
     * 变量类型与定义方式是多对多的（见 {@link VariableDefinitionConfig#TYPE_DEFINITIONS}），
     * 表单上必须只列出与当前类型兼容的那些。类型如今是子类的静态属性，因此收敛脚本可以
     * 在<strong>构造期</strong>写死，不再需要 {@code MultiStepsSupportHost} 那套读取上一步实例的
     * 运行时上下文。
     *
     * <p>TIS 没有 descriptor 层的候选过滤钩子，唯一的注入口是字段级 extra props 上的
     * {@code subDescEnumFilter}（消费方 {@code PropertyType.applicableDescriptors(boolean)}）。
     * 本构造器直接向 {@link Descriptor#fieldExtraDescs} 写入该键：
     * {@code PluginExtraProps.load()} 会把 {@code fieldExtraDescs} <b>最后</b>合并到各类
     * {@code .json} 之上，因此基类 {@code WorkshopVariable.json} 里 {@code definitionConfig}
     * 的 label / help 会被保留（合并是按 key 递归深合并），这里只需加一个叶子键。
     *
     * <p>Groovy 产物的类名是 {@code <子类 SimpleName>_definitionConfig_SubFilter} —— 逐子类唯一，
     * 不会在 Groovy 的类缓存里互相覆盖（{@code PropertyType} 取的是具体子类作为 ownerClazz）。
     * 脚本里必须用 <b>FQCN</b> 引用目标类：框架的模板只 import 了
     * {@code Function} / {@code List} / {@code Descriptor}。
     *
     * <p>注意脚本里的形参名固定为 {@code desc}（见 {@code PropertyType} 的模板）。
     */
    public abstract static class BasicDescriptor extends Descriptor<WorkshopVariable>
            implements DescriptorUseableShortComment, IEndTypeGetter {

        private final VariableType variableType;

        protected BasicDescriptor(VariableType variableType) {
            this.variableType = Objects.requireNonNull(variableType, "variableType can not be null");

            JSONObject props = new JSONObject();
            props.put(PluginExtraProps.KEY_ENUM_FILTER,
                    "return " + VariableDefinitionConfig.class.getName() + ".applicableDefinitions("
                            + VariableType.class.getName() + "." + variableType.name() + ", desc);");
            // 类型已是静态事实，help 可一并写死；覆盖基类 json 里的通用兜底文案
            props.put(Option.KEY_HELP,
                    "为'" + variableType.shortComment() + "'类型选择定义方式");
            this.fieldExtraDescs.put(KEY_DEFINITION_CONFIG, new PluginExtraProps.Props(props));
        }

        public VariableType getVariableType() {
            return this.variableType;
        }

        /**
         * 插件选择器上显示的名称，如「字符串变量」
         */
        @Override
        public String getDisplayName() {
            return this.variableType.name();
        }

        /**
         * 供变量类型选择器展示的一句话说明，直接复用枚举上已有的中文短描述，
         * 类型的中文名因此只维护一份。
         */
        @Override
        public String shortComment() {
            return this.variableType.shortComment() + "类型参数";
        }
    }
}
