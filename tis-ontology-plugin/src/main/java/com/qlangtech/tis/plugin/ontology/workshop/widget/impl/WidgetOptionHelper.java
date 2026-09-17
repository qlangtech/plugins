package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.manage.common.OptionWithEndType;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.ontology.Ontology;
import com.qlangtech.tis.plugin.ontology.OntologyDomain;
import com.qlangtech.tis.plugin.ontology.OntologyObjectType;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.plugin.ontology.workshop.WorkshopModule;
import com.qlangtech.tis.plugin.ontology.workshop.enums.VariableType;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopPage;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopVariable;
import com.qlangtech.tis.plugin.ontology.workshop.model.definition.ObjectSetDefinitionConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.definition.VariableDefinitionConfig;
import com.qlangtech.tis.plugin.ontology.workshop.store.WorkshopPageStore;
import com.qlangtech.tis.util.IPluginContext;
import com.qlangtech.tis.util.UploadPluginMeta;
import org.apache.commons.lang.StringUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Widget SELECTABLE 选项提供者工具类
 * <p>
 * 两类选项：其一，各 Widget 变量绑定字段的候选变量名（按 {@link VariableType} 过滤，
 * 见 {@link #getVariableOptions(VariableType...)} 及其一组薄包装）；其二，按已选变量
 * 级联出的属性列表（见 {@link #getObjectPropertyOptions(String, OntologyPluginMeta)}）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/11
 */
public class WidgetOptionHelper {

    /**
     * UploadPluginMeta 额外参数键：Workshop Module 名称
     * 前端在请求 Widget 配置表单时需要传入此参数以定位当前 Module 上下文
     */
    public static final String KEY_WORKSHOP_MODULE_NAME = "workshopModuleName";

    private WidgetOptionHelper() {
    }

    // ===============================================================
    //  当前表单上下文 —— 两套 extraParam 键的容错读取
    // ===============================================================

    /**
     * 当前插件表单上下文中的 ontology domain id，取不到返回 null。
     *
     * <h3>为什么两组键都认</h3>
     * 同一个 domain 在不同表单里用的 extraParam 键不同（已在前端逐一核实）：
     * <table>
     *   <tr><th>链路</th><th>domain 键</th><th>module 键</th></tr>
     *   <tr><td>Widget 配置表单</td><td>{@code ontology_<x>}</td>
     *       <td>{@code workshopModuleName_<y>}</td></tr>
     *   <tr><td>变量配置表单（{@code variable-manager.component.ts} 的
     *       {@code createPluginType()}）</td><td>{@code ontologyDomainId_<x>}</td>
     *       <td>{@code moduleName_<y>}</td></tr>
     * </table>
     * 本方法两组都试，故两种调用方都能用。
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
     * 当前插件表单上下文中的 Workshop Module 名称，取不到返回 null。两组键的说明同
     * {@link #getCurrentOntologyDomainId()}。
     */
    public static String getCurrentWorkshopModuleName() {
        UploadPluginMeta pluginMeta = currentPluginMeta();
        if (pluginMeta == null) {
            return null;
        }
        String moduleName = pluginMeta.getExtraParam(WorkshopVariable.PARAM_MODULE_NAME);
        if (StringUtils.isEmpty(moduleName)) {
            moduleName = pluginMeta.getExtraParam(KEY_WORKSHOP_MODULE_NAME);
        }
        return moduleName;
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
     * 当前 domain 下的<b>全部页面</b> —— 供 {@code PageRoutingConfig.targetPage} 的下拉使用。
     *
     * <p>页面按 domain 归属、不按模块归属，故这里只用到 domain。
     *
     * <p>选项的 {@code value} 用 {@code page.name} 而非 {@code page.id}：与变量一致，
     * 模块内引用一律用<b>名字</b>（{@code page.id} 是自动生成的 UUID，只决定落盘文件名）。
     * {@code label} 用 {@code displayName}，为空时回落到 {@code name}。
     *
     * @return 页面选项列表；无法解析上下文时为空列表
     */
    public static List<IdentityName> getWorkshopPageOptions() {
        String domain = getCurrentOntologyDomainId();
        if (StringUtils.isEmpty(domain)) {
            return Collections.emptyList();
        }
        try {
            List<IdentityName> options = new ArrayList<>();
            // 直接走 store：本方法是静态工具，为一次渲染新建一个 service 实例没有意义
            for (WorkshopPage page : WorkshopPageStore.create(domain).listAll()) {
                String label = StringUtils.isEmpty(page.displayName) ? page.name : page.displayName;
                options.add(new Option(label, page.name));
            }
            return options;
        } catch (Throwable t) {
            return Collections.emptyList();
        }
    }

    // ===============================================================
    //  Public API — 变量绑定字段的 SELECTABLE 选项
    // ===============================================================

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
    public static List<IdentityName> getVariableOptions(VariableType... types) {
        Set<VariableType> accepted = EnumSet.noneOf(VariableType.class);
        Collections.addAll(accepted, types);
        List<WorkshopVariable> variables = loadCurrentModuleVariables();
        if (variables == null) {
            return Collections.emptyList();
        }
        return variables.stream()
                // 类型由具体子类固化，恒非 null，无需再判空
                .filter(v -> accepted.contains(v.getVariableType()))
                .map(v -> IdentityName.create(v.getName()))
                .collect(Collectors.toList());
    }

    /** 对象集合：ObjectTableWidget / ObjectListWidget 的 objectSetVar，ObjectSetDataInput.dataSourceVar */
    public static List<IdentityName> getObjectSetVariableOptions() {
        return getVariableOptions(VariableType.OBJECT_SET);
    }

    /** 字符串：HeaderTextWidget.textVariable、TextInputWidget 的两个绑定、MarkdownWidget.contentVar */
    public static List<IdentityName> getStringVariableOptions() {
        return getVariableOptions(VariableType.STRING);
    }

    /** 数值：MetricCardWidget.valueVar */
    public static List<IdentityName> getNumericVariableOptions() {
        return getVariableOptions(VariableType.NUMERIC);
    }

    /** 布尔：CheckboxWidget.checkedVar */
    public static List<IdentityName> getBooleanVariableOptions() {
        return getVariableOptions(VariableType.BOOLEAN);
    }

    /**
     * 日期 / 时间戳：DateTimePickerWidget.dateValueVar。
     *
     * <p>同时接受两种类型 —— 该 Widget 的 {@code variableKind}（iso / timestamp）只决定
     * <b>写出的值</b>的格式，不决定目标变量应有的类型，故两者都列出由使用者自行选择。
     */
    public static List<IdentityName> getDateTimeVariableOptions() {
        return getVariableOptions(VariableType.DATE, VariableType.TIMESTAMP);
    }

    /** 对象集合过滤器：FilterListWidget.filterOutputVar */
    public static List<IdentityName> getObjectSetFilterVariableOptions() {
        return getVariableOptions(VariableType.OBJECT_SET_FILTER);
    }

    /** 时间序列集合：TimelineWidget.dataSourceVar */
    public static List<IdentityName> getTimeSeriesSetVariableOptions() {
        return getVariableOptions(VariableType.TIME_SERIES_SET);
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
        String moduleName = pluginMeta.getDelegate().getExtraParam(KEY_WORKSHOP_MODULE_NAME);
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
            if (!(defConfig instanceof ObjectSetDefinitionConfig)) {
                return Collections.emptyList();
            }

            String objectTypeName = ((ObjectSetDefinitionConfig) defConfig).objectType;
            if (StringUtils.isEmpty(objectTypeName)) {
                return Collections.emptyList();
            }

            OntologyObjectType objType = Ontology.loadObjectTypeDetail(domain, objectTypeName);
            if (objType == null) {
                return Collections.emptyList();
            }
            return objType.getColOpts();

        } catch (Exception e) {
            // 任何加载异常静默返回空列表
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
        String moduleName = meta.getDelegate().getExtraParam(KEY_WORKSHOP_MODULE_NAME);
        if (StringUtils.isEmpty(domain) || StringUtils.isEmpty(moduleName)) {
            throw new IllegalStateException("param domain or moduleName can not be null");
        }
        WorkshopModule module = WorkshopModule.load(domain, moduleName);
        return module != null ? module.variables : null;
//        } catch (Exception e) {
//            return null;
//        }
    }
}