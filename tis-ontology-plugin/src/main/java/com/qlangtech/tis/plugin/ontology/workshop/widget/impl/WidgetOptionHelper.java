package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.qlangtech.tis.manage.common.OptionWithEndType;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.ontology.Ontology;
import com.qlangtech.tis.plugin.ontology.OntologyObjectType;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.plugin.ontology.workshop.WorkshopModule;
import com.qlangtech.tis.plugin.ontology.workshop.enums.VariableType;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopVariable;
import com.qlangtech.tis.plugin.ontology.workshop.model.definition.ObjectSetDefinitionConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.definition.VariableDefinitionConfig;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.lang.StringUtils;

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
                .filter(v -> v.getType() != null && accepted.contains(v.getType()))
                .map(v -> IdentityName.create(v.getName()))
                .collect(Collectors.toList());
    }

    /** 对象集合：ObjectTableWidget / ObjectListWidget 的 objectSetVar，ChartLayer.dataSourceVar */
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