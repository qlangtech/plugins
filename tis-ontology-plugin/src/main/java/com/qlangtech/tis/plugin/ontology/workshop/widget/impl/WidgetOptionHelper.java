package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorExtensionList;
import com.qlangtech.tis.extension.util.PluginExtraProps;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.manage.common.OptionWithEndType;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.ontology.Ontology;
import com.qlangtech.tis.plugin.ontology.OntologyAction;
import com.qlangtech.tis.plugin.ontology.OntologyDomain;
import com.qlangtech.tis.plugin.ontology.OntologyObjectType;
import com.qlangtech.tis.plugin.ontology.OntologyProperty;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.plugin.ontology.workshop.WorkshopModule;
import com.qlangtech.tis.plugin.ontology.workshop.enums.VariableType;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopOverlay;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopPage;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopSection;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopVariable;
import com.qlangtech.tis.plugin.ontology.workshop.model.definition.ObjectSetDefinitionConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.definition.VariableDefinitionConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.variable.ObjectSetVariable;
import com.qlangtech.tis.util.IPluginContext;
import com.qlangtech.tis.util.UploadPluginMeta;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/**
 * Widget SELECTABLE 选项提供者工具类
 * <p>
 * 两类选项：其一，各 Widget 变量绑定字段的候选变量（按 {@link VariableType} 过滤，
 * 见 {@link #getVariableOptions(VariableType...)} 与返回 {@link PluginExtraProps.FieldRefConfig}
 * 的一组薄包装——字段由此获得「配置」跳转 creator）；其二，按已选变量
 * 级联出的属性列表（见 {@link #getObjectPropertyOptions(String, OntologyPluginMeta)}）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/11
 */
public class WidgetOptionHelper {

    private static final Logger logger = LoggerFactory.getLogger(WidgetOptionHelper.class);

    private WidgetOptionHelper() {
    }

    // ===============================================================
    //  当前表单上下文 —— extraParam 读取
    // ===============================================================

    /**
     * 当前插件表单上下文中的 ontology domain id，取不到返回 null。
     *
     * <h3>domain 键的两组容错</h3>
     * 同一个 domain 在不同表单里用的 extraParam 键不同：
     * <table>
     *   <tr><th>链路</th><th>domain 键</th><th>module 键</th></tr>
     *   <tr><td>Widget 配置表单</td><td>{@code ontology_<x>}</td>
     *       <td>{@code workshop_<y>}</td></tr>
     *   <tr><td>变量配置表单（{@code variable-manager.component.ts} 的
     *       {@code createPluginType()}）</td><td>{@code ontologyDomainId_<x>}</td>
     *       <td>{@code workshop_<y>}</td></tr>
     * </table>
     * module 键已统一为 {@link OntologyDomain#KEY_WORKSHOP}（{@code workshop}），
     * 只有 domain 键仍需两组都试。
     *
     * <h3>为什么取不到时返回 null 而不是抛异常</h3>
     * {@link #loadCurrentModuleVariables()} 在 domain/module 为空时直接
     * {@code throw new IllegalStateException(...)}。那对「用户点开 Widget 配置」是合理的
     * ——上下文缺失说明前端接线错了。但本方法服务于<b>渲染期</b>的选项求值：抛异常会让
     * <b>整张表单一打开就报错</b>，而缺一个下拉的候选只是降级。故一律返回 null/空列表，
     * 与 {@link #getObjectPropertyOptions(String, OntologyPluginMeta)} 的兜底口径一致。
     */
    public static String getCurrentOntologyDomainId() {
        UploadPluginMeta pluginMeta = currentPluginMeta();
        if (pluginMeta == null) {
            return null;
        }
        String domain = pluginMeta.getExtraParam(WorkshopVariable.PARAM_ONTOLOGY_DOMAIN_ID);
        if (StringUtils.isEmpty(domain)) {
            domain = pluginMeta.getExtraParam(OntologyDomain.NAME_ONTOLOGY_DOMAIN);
        }
        return domain;
    }

    /**
     * 当前插件表单上下文中的 Workshop Module 名称，取不到返回 null。
     */
    public static String getCurrentWorkshopModuleName() {
        UploadPluginMeta pluginMeta = currentPluginMeta();
        if (pluginMeta == null) {
            return null;
        }
        return pluginMeta.getExtraParam(OntologyDomain.KEY_WORKSHOP);
    }

    private static UploadPluginMeta currentPluginMeta() {
        try {
            IPluginContext pluginContext = IPluginContext.getThreadLocalInstance();
            if (pluginContext == null) {
                return null;
            }
            // validateNull=false：拿不到插件元数据时要降级为 null，不能在这里抛 NPE
            return UploadPluginMeta.createPluginMeta(pluginContext.getContext(), false);
        } catch (Throwable t) {
            return null;
        }
    }

    // ===============================================================
    //  Public API — 变量 / 页面配置表单自身的 SELECTABLE 选项
    // ===============================================================

    /**
     * 当前 Workshop Module 里的<b>全部</b>变量名（不按类型过滤）—— 供<b>变量配置表单自身</b>
     * 的下拉使用（如 {@code InterfaceInputRow.variableName} 那一列）。
     *
     * <p>读上下文的两组键见 {@link #getCurrentOntologyDomainId()}。
     *
     * @return IdentityName 列表，{@code identityValue()} 即变量名；无法解析上下文时为空列表
     */
    public static List<IdentityName> getWorkshopVariableOptions() {
        return loadVariablesForVariableForm().stream()
                .map(v -> IdentityName.create(v.getName()))
                .collect(Collectors.toList());
    }

    private static List<WorkshopVariable> loadVariablesForVariableForm() {
        try {
            String domain = getCurrentOntologyDomainId();
            String moduleName = getCurrentWorkshopModuleName();
            if (StringUtils.isEmpty(domain) || StringUtils.isEmpty(moduleName)) {
                return Collections.emptyList();
            }

            WorkshopModule module = WorkshopModule.load(domain, moduleName);
            if (module == null || module.variables == null) {
                return Collections.emptyList();
            }
            return module.variables;
        } catch (Throwable t) {
            // 渲染期选项求值失败只降级为空候选，不阻断表单
            return Collections.emptyList();
        }
    }

    /**
     * 当前模块下的<b>全部页面</b> —— 供 {@code PageRoutingConfig.targetPage} 与
     * {@code SwitchToPageConfig} 的下拉使用。
     *
     * <p>页面是模块的聚合子实体，落盘在 {@code workshop/{moduleName}/pages/} 下，
     * 因此上下文必须同时有 domain 与 moduleName；缺任一个都返回空列表。
     *
     * <p>选项的 {@code value} 用 {@code page.id}：页面一律<b>按 id</b> 寻址，
     * name 是可改的显示名。{@code label} 用 {@code displayName}，为空时回落到 {@code name}。
     *
     * @return 页面选项列表；无法解析上下文时为空列表
     */
    public static List<IdentityName> getWorkshopPageOptions() {
        String domain = getCurrentOntologyDomainId();
        String moduleName = getCurrentWorkshopModuleName();
        if (StringUtils.isEmpty(domain) || StringUtils.isEmpty(moduleName)) {
            return Collections.emptyList();
        }
        try {
            List<IdentityName> options = new ArrayList<>();
            // 直接走 store：本方法是静态工具，为一次渲染新建一个 service 实例没有意义
            for (WorkshopPage page : WorkshopModule.load(domain, moduleName).loadAllPage()) {
                // 跳过没有 id 的坏数据：单条 item 抛异常会被外层 catch 把整个候选列表降级为空
                if (StringUtils.isEmpty(page.id)) {
                    continue;
                }
                String label = StringUtils.isEmpty(page.displayName) ? page.name : page.displayName;
                options.add(new Option(label, page.id));
            }
            return options;
        } catch (Throwable t) {
            return Collections.emptyList();
        }
    }

    // ===============================================================
    //  Public API — 变量绑定字段的 SELECTABLE 选项
    // ===============================================================
    public static List<IdentityName> getVariableOptions(VariableType... types) {
        return getVariableOptions((var) -> true, types);
    }

    /**
     * 按变量类型取当前 Workshop Module 中的变量名列表 —— 所有变量绑定字段选项的唯一实现。
     *
     * <p>各 Widget 的变量绑定字段（{@code objectSetVar} / {@code dataSourceVar} /
     * {@code activeObjectVar} …）都是 {@code FormFieldType.SELECTABLE}，选项即「当前模块中
     * 类型匹配的变量名」。各角色之间唯一的差异就是接受的<b>变量类型</b>，故这里按
     * {@code types} 过滤，其余逻辑只此一处维护。
     *
     * @param types 接受的变量类型；传多个表示「任意其一」
     *              （如日期选择器同时接受 DATE 与 TIMESTAMP）
     * @return IdentityName 列表，每个元素的 identityValue() 即为变量名
     */
    public static List<IdentityName> getVariableOptions(Predicate<WorkshopVariable> predicate, VariableType... types) {
        Set<VariableType> accepted = EnumSet.noneOf(VariableType.class);
        Collections.addAll(accepted, types);
        List<WorkshopVariable> variables = loadCurrentModuleVariables();
        if (variables == null) {
            return Collections.emptyList();
        }

        return variables.stream()
                // 类型由具体子类固化，恒非 null，无需再判空
                .filter(v -> accepted.contains(v.getVariableType()) && predicate.test(v))
                .map(v -> {
                    VariableType varType = v.getVariableType();
                    OptionWithEndType endType = new OptionWithEndType(v.getName(), v.getName(), varType.getEndType());
                    if (types.length > 1) {
                        endType.setComment(new Option.OptionComment(Option.OptionCommentColor.forObject(varType), varType.shortComment()));
                    }
                    return endType;
                })
                .collect(Collectors.toList());
    }

    /**
     * 可作为<b>查询数据源</b>的对象集合变量：只列「对象集合定义」这一种定义方式。
     *
     * <p>OBJECT_SET 类型现在也接受 {@code ExternalInputConfig}（由行点击等交互写入的选中行
     * 载体），它没有可执行的查询语义；若把它填进 {@code objectSetVar}，取数时会在
     * {@code ObjectSetVariable#resolveDataSourceBinding} 抛 illegal type。此方法把这类变量
     * 从「数据源」下拉里排除，同时保留在 {@code activeObjectVar} / {@code selectedObjectsVar}
     * 的候选里（那些字段正是要写外部值）。
     *
     * <p>creator 候选与 {@link #getObjectSetVariableOptions()} 一致（均为 OBJECT_SET
     * 类型的变量插件），差异只在选项过滤：数据源排除「外部写入」型变量。
     */
    public static PluginExtraProps.FieldRefConfig getObjectSetSourceVariableOptions() {
        return getFieldRefConfig((var) -> var.getDefinitionConfig() instanceof ObjectSetDefinitionConfig
                , VariableType.OBJECT_SET);
    }

    /**
     * 对象集合：ObjectTableWidget / ObjectListWidget 的 objectSetVar，ChartLayer.dataSourceVar
     */
    public static PluginExtraProps.FieldRefConfig getObjectSetVariableOptions() {
        return getFieldRefConfig(VariableType.OBJECT_SET);
    }

    private static PluginExtraProps.FieldRefConfig getFieldRefConfig(final VariableType... types) {
        return getFieldRefConfig((var) -> true, types);
    }

    /**
     * 构建变量绑定字段的 {@link PluginExtraProps.FieldRefConfig}：creator（「配置」hyperlink，
     * 候选为 accepted 类型对应的变量插件）与选项（按 predicate + types 过滤的现有变量）
     * 都<b>惰性</b>构建 —— descriptor 构造期（如单测里直接 new、插件扫描期）零重逻辑，
     * 首次访问（表单元数据下发，运行期）才触发 TIS 描述符扫描与变量加载。
     *
     * @param predicate 选项级变量过滤（如「数据源」下拉要排除「外部写入」型变量）
     * @param types     creator 候选与选项共用的变量类型集合；传多个表示「任意其一」
     */
    private static PluginExtraProps.FieldRefConfig getFieldRefConfig(
            Predicate<WorkshopVariable> predicate, final VariableType... types) {
        if (types == null || types.length == 0) {
            throw new IllegalArgumentException("param types can not be empty");
        }
        Set<VariableType> accepted = EnumSet.noneOf(VariableType.class);
        Collections.addAll(accepted, types);
        return PluginExtraProps.FieldRefConfig.lazy(() -> createVariableCreator(accepted)
                , () -> getVariableOptions(predicate, types));
    }

    /**
     * 按 accepted 类型集收集变量插件候选。「配置」hyperlink 跳转到对应变量插件的
     * 创建页，故凡类型匹配的 {@link WorkshopVariable.BasicDescriptor} 都进候选。
     */
    private static PluginExtraProps.FieldRefCreator createVariableCreator(Set<VariableType> accepted) {
        PluginExtraProps.FieldRefCreator creator = new PluginExtraProps.FieldRefCreator();
      //  creator.setLabel("配置");
        creator.setAssistType(PluginExtraProps.RouterAssistType.paramCfg);


        DescriptorExtensionList<WorkshopVariable, Descriptor<WorkshopVariable>> variableDesc
                = TIS.get().getDescriptorList(WorkshopVariable.class);
        OntologyPluginMeta meta = OntologyPluginMeta.createPluginMeta();
        for (Descriptor<WorkshopVariable> d : variableDesc) {
            if (!(d instanceof WorkshopVariable.BasicDescriptor desc)) {
                throw new IllegalStateException("variable desc must be " + WorkshopVariable.BasicDescriptor.class);
            }
            if (accepted.contains(desc.getVariableType())) {
                PluginExtraProps.CandidatePlugin candidatePlugin = new PluginExtraProps.CandidatePlugin(
                        desc.getDisplayName(), Optional.empty(), WorkshopVariable.workshopVariable.identity);
                if (StringUtils.isEmpty(meta.getDomain())) {
                    throw new IllegalStateException("domain can not be empty");
                }
                if (StringUtils.isEmpty(meta.getDelegate().getExtraParam(OntologyDomain.KEY_WORKSHOP))) {
                    throw new IllegalStateException("workshop can not be empty");
                }
                candidatePlugin.setExtraParam(meta.getDelegate().serialExtraParams());
                creator.addCandidatePlugin(candidatePlugin);
            }
        }
        return creator.disableEnableManage();
    }


    /**
     * 字符串：HeaderTextWidget.textVariable、TextInputWidget 的两个绑定、MarkdownWidget.contentVar
     */
    public static PluginExtraProps.FieldRefConfig getStringVariableOptions() {
        return getFieldRefConfig(VariableType.STRING);
    }

    /**
     * 数值型变量。当前无调用方 —— 数值绑定走 {@code MetricConfig.valueVariable} 的
     * STRING+NUMERIC 组合注册；保留以维持变量类型包装族的完整性。
     */
    public static PluginExtraProps.FieldRefConfig getNumericVariableOptions() {
        return getFieldRefConfig(VariableType.NUMERIC);
    }

    /**
     * 布尔：CheckboxWidget.checkedVar
     */
    public static PluginExtraProps.FieldRefConfig getBooleanVariableOptions() {
        return getFieldRefConfig(VariableType.BOOLEAN);
    }

    /**
     * 日期 / 时间戳：DateTimePickerWidget.dateValueVar、DateInputWidget 的两个绑定。
     *
     * <p>同时接受两种类型 —— 该 Widget 的 {@code variableKind}（iso / timestamp）只决定
     * <b>写出的值</b>的格式，不决定目标变量应有的类型，故两者都列出由使用者自行选择。
     */
    public static PluginExtraProps.FieldRefConfig getDateTimeVariableOptions() {
        return getFieldRefConfig(VariableType.DATE, VariableType.TIMESTAMP);
    }

    /**
     * 对象集合过滤器：FilterListWidget.filterOutputVar、ObjectSetDefinitionConfig.filterVar
     */
    public static PluginExtraProps.FieldRefConfig getObjectSetFilterVariableOptions() {
        return getFieldRefConfig(VariableType.OBJECT_SET_FILTER);
    }

    /**
     * 时间序列集合：TimelineWidget.dataSourceVar
     */
    public static PluginExtraProps.FieldRefConfig getTimeSeriesSetVariableOptions() {
        return getFieldRefConfig(VariableType.TIME_SERIES_SET);
    }

    /**
     * 当前 Workshop Module 里的<b>全部浮层</b> —— 供事件配置（{@code open-overlay} /
     * {@code close-overlay}）的 {@code overlayName} 下拉使用。
     *
     * <p>与页面一样，选项的 {@code value} 用名字而非自动生成的 UUID。
     *
     * @return 浮层选项列表；无法解析上下文时为空列表
     */
    public static List<IdentityName> getWorkshopOverlayOptions() {
        WorkshopModule module = loadCurrentModuleQuietly();
        if (module == null || module.overlays == null) {
            return Collections.emptyList();
        }
        List<IdentityName> options = new ArrayList<>();
        for (WorkshopOverlay overlay : module.overlays) {
            options.add(new Option(overlay.name, overlay.name));
        }
        return options;
    }

    /**
     * 当前 Workshop Module 里<b>全部页面下的全部区块</b> —— 供事件配置
     * （{@code expand-section} / {@code collapse-section} / {@code toggle-section}）的
     * {@code sectionId} 下拉使用。
     *
     * <p>区块隶属于页面（{@code WorkshopPage.sections}），故这里要把所有页面摊平。
     * 选项的 {@code value} 用 {@code section.id}（UUID 主键，与前端 LayoutService 的
     * 区块寻址键一致），{@code label} 用 {@code section.name}。
     *
     * @return 区块选项列表；无法解析上下文时为空列表
     */
    public static List<IdentityName> getWorkshopSectionOptions() {
        WorkshopModule module = loadCurrentModuleQuietly();
        if (module == null || module.pages == null) {
            return Collections.emptyList();
        }
        List<IdentityName> options = new ArrayList<>();
        for (WorkshopPage page : module.pages) {
            if (page.sections == null) {
                continue;
            }
            for (WorkshopSection section : page.sections) {
                options.add(new Option(section.name, section.identityValue()));
            }
        }
        return options;
    }

    /**
     * 渲染期的「安静」版模块加载 —— 取不到上下文/加载失败一律返回 null，不抛异常。
     *
     * <p>与 {@link #loadCurrentModuleVariables()} 相对：那个是「用户点开配置表单」路径，上下文
     * 缺失说明前端接线错了，抛异常是合理的；本方法服务的是<b>选项求值</b>，抛异常会让整张表单
     * 一打开就报错，而缺一个下拉候选只是降级。
     */
    private static WorkshopModule loadCurrentModuleQuietly() {
        try {
            String domain = getCurrentOntologyDomainId();
            String moduleName = getCurrentWorkshopModuleName();
            if (StringUtils.isEmpty(domain) || StringUtils.isEmpty(moduleName)) {
                return null;
            }
            return WorkshopModule.load(domain, moduleName);
        } catch (Throwable t) {
            return null;
        }
    }

    // ===============================================================
    //  Public API — 本体 Action 选项
    // ===============================================================

    /**
     * 当前本体域中已定义的<b>全部 Action</b> —— 供所有「触发本体动作」字段的选项：
     * {@code WidgetActionConfig.actionId}、{@code OntologyActionClickAction.actionId}。
     *
     * <p>与变量下拉不同，Action 不属于 Workshop Module 而属于<b>本体域</b>，故只需 domain
     * 上下文（{@link #getCurrentOntologyDomainId()}），不需要 module 名。
     *
     * <p>选项的 {@code value} 用 {@code action.getName()} 而非其它标识：与变量/页面一致，
     * 模块内引用一律用<b>名字</b>。{@code label} 用 {@code getDisplayName()}，
     * 为空时回落到 {@code getName()}。
     *
     * <p>取不到上下文或加载失败一律返回空列表，不抛异常 —— 渲染期抛异常会让整张表单打不开，
     * 与 {@link #getObjectPropertyOptions(String, OntologyPluginMeta)} 的兜底口径一致。
     */
    public static List<IdentityName> getOntologyActionOptions() {
        String domain = getCurrentOntologyDomainId();
        if (StringUtils.isEmpty(domain)) {
            return Collections.emptyList();
        }
        try {
            List<IdentityName> options = new ArrayList<>();
            for (OntologyAction action : Ontology.loadAllActions(domain)) {
                String name = action.getName();
                String label = StringUtils.isEmpty(action.getDisplayName()) ? name : action.getDisplayName();
                options.add(new Option(label, name));
            }
            return options;
        } catch (Throwable t) {
            return Collections.emptyList();
        }
    }

    /**
     * 根据选中的 objectSetVar 变量名，解析其 ObjectSetDefinitionConfig 所引用的
     * OntologyObjectType 属性列表，供 labelProp / startProp / endProp / rowProperty
     * 等级联字段使用。
     *
     * @param objectSetVarName 用户选中的对象集变量名
     * @param pluginMeta       当前插件的 UploadPluginMeta（含 domain 上下文）
     * @return 属性选项列表
     */
    public static List<OptionWithEndType> getObjectPropertyOptions(
            String objectSetVarName, OntologyPluginMeta pluginMeta) {
        if (StringUtils.isEmpty(objectSetVarName) || pluginMeta == null) {
            return Collections.emptyList();
        }

        String domain = pluginMeta.getDomain();
        String moduleName = pluginMeta.getDelegate().getExtraParam(OntologyDomain.KEY_WORKSHOP);
        if (StringUtils.isEmpty(domain) || StringUtils.isEmpty(moduleName)) {
            return Collections.emptyList();
        }

        try {
            WorkshopModule module = WorkshopModule.load(domain, moduleName);
            if (module == null) {
                return Collections.emptyList();
            }
            WorkshopVariable variable = module.findVariableByName(objectSetVarName);
            if (variable == null) {
                return Collections.emptyList();
            }

            VariableDefinitionConfig defConfig = variable.getDefinitionConfig();
            if (!(defConfig instanceof ObjectSetDefinitionConfig objSetDef)) {
                return Collections.emptyList();
            }

            String objectTypeName = objSetDef.objectType;
            if (StringUtils.isEmpty(objectTypeName)) {
                return Collections.emptyList();
            }

            OntologyObjectType objType = Ontology.loadObjectTypeDetail(domain, objectTypeName);
            if (objType == null) {
                return Collections.emptyList();
            }
            return objType.getColOpts();

        } catch (Exception e) {
            // 任何加载异常静默返回空列表（渲染期降级，不阻断表单）
            //
            // 口径与 getObjectPropertyMetas 现在一致：后者对「取不到 schema」的变量
            // （ExternalInput / FunctionConfig）同样降级为空候选，只对「不是对象集合变量」
            // 这种真正的配置错误抛。于是同一个 descriptor 里 titleProperty 与 cardFields
            // 不会再一个静默为空、一个直接 500。区别只在因果粒度：这里是宽 catch
            // （连磁盘加载失败也吞），那里只兜 getColsMeta 抛出的 schema 缺失。
            return Collections.emptyList();
        }
    }

    /**
     * 取对象集合变量的完整属性定义（{@link OntologyProperty}，含 {@code parseOntologyType()}），
     * 供 {@code WidgetColumnConfig.createFrom} 推导列配置使用。
     *
     * <p>与 {@link #getObjectPropertyOptions(String, OntologyPluginMeta)} 的分工：那个返回的是
     * 下拉用的 {@code OptionWithEndType}（只需要名字与类型标签），本方法返回的是完整属性元数据
     * ——列配置还需要 description 与类型枚举。也正因此<b>没有</b>把前者重构为调用本方法：
     * 前者为渲染期降级而宽 catch，会把这里要对用户暴露的错误吞掉。
     *
     * <h3>取不到 schema 时降级为空候选，<b>不抛</b></h3>
     * 本方法的两个调用方都是<b>渲染候选</b>（{@code valueChangePipe} 的 render，与
     * {@code WidgetColumnConfig.DftDescriptor#getEnumableCandidateSet}）。渲染期抛异常会被
     * {@code RootFormProperties#getInstancePropsJson} 包成 {@code RuntimeException}，
     * 结果是<b>整个模块 GET 500、widget 配置表单打不开</b>。
     *
     * <p>而「取不到 schema」是<b>正常配置</b>而非错误：只有 {@code ObjectSetDefinitionConfig}
     * 才有属性清单；{@code ExternalInputConfig}（外部写入，接收行选中项的典型载体）与
     * {@code FunctionConfig}（输出 schema 未建模）都会让
     * {@link ObjectSetVariable#getColsMeta()} 抛 {@code IllegalStateException}。
     * 用户把这类变量填进「对象集变量」下拉是完全合法的（下拉本身就是全部 OBJECT_SET）。
     *
     * <p>因此这里**不按子类硬编码**（{@code instanceof ExternalInputConfig} 这种写法在下一个
     * 新增定义方式时会静默复发），而是按「有没有 schema」这一唯一判据降级：
     * <pre>
     * try { return objSetVar.getColsMeta(); }
     * catch (IllegalStateException e) { logger.warn(...); return emptyList(); }
     * </pre>
     *
     * <p>⚠️ {@code ObjectSetVariable#getColsMeta()} 本身**保持抛**（取数路径
     * {@code resolveDataSourceBinding} 需要可执行的错误），降级只发生在渲染候选这一层。
     *
     * @param objectSetVarName 用户选中的对象集合变量名，可能为 null / 空
     * @return 属性列表；变量名为空、变量已不存在、或该变量取不到 schema 时返回空列表
     * （软失败，表单仍能打开，属性行由用户手填）
     * @throws IllegalStateException 变量存在但类型不是对象集合变量（真正的配置错误）
     */
    public static List<OntologyProperty> getObjectPropertyMetas(String objectSetVarName) {
        if (StringUtils.isEmpty(objectSetVarName)) {
            return Collections.emptyList();
        }
        WorkshopVariable variable = WorkshopVariable.loadDetail(objectSetVarName);
        if (variable == null) {
            return Collections.emptyList();
        }
        if (!(variable instanceof ObjectSetVariable objSetVar)) {
            throw new IllegalStateException("变量 '" + objectSetVarName + "' 的类型为 "
                    + variable.getClass().getSimpleName() + "，不是对象集合变量，无法推导列定义");
        }
        try {
            return objSetVar.getColsMeta();
        } catch (IllegalStateException e) {
            // 外部写入 / 函数计算型对象集没有属性清单：候选为空，属性行由用户手填。
            // 降级而不是抛出，理由见方法注释（渲染期抛出 = 整个表单 500）。
            logger.warn("对象集合变量 '{}' 无法推导属性清单，候选降级为空：{}", objectSetVarName, e.getMessage());
            return Collections.emptyList();
        }
    }

    // ===============================================================
    //  Private helpers
    // ===============================================================

    /**
     * 从线程上下文获取当前 WorkshopModule，返回其变量列表
     */
    private static List<WorkshopVariable> loadCurrentModuleVariables() {
        IPluginContext pluginContext = IPluginContext.getThreadLocalInstance();
//        if (pluginContext == null) {
//            throw new IllegalStateException();
//        }
        // try {
        OntologyPluginMeta meta = OntologyPluginMeta.createPluginMeta(pluginContext.getContext());
        String domain = meta.getDomain();
        String moduleName = meta.getDelegate().getExtraParam(OntologyDomain.KEY_WORKSHOP);
        if (StringUtils.isEmpty(domain) || StringUtils.isEmpty(moduleName)) {
            throw new IllegalStateException("param domain or moduleName can not be null");
        }

        return WorkshopVariable.loadAll(domain, moduleName);

//        WorkshopModule module = WorkshopModule.load(domain, moduleName);
//        return module != null ? module.variables : null;
//        } catch (Exception e) {
//            return null;
//        }
    }
}