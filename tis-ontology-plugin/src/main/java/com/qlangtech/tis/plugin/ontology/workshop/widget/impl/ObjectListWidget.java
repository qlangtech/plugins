package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.plugin.ontology.workshop.widget.ObjectListDisplay;
import com.qlangtech.tis.plugin.ontology.workshop.widget.WidgetActionConfig;
import com.qlangtech.tis.plugin.ontology.workshop.widget.WidgetColumnConfig;
import com.qlangtech.tis.plugin.ontology.workshop.widget.FullDisplayWidget;
import com.qlangtech.tis.plugin.ontology.workshop.widget.IWidgetColumnHost;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * P0 Widget：对象列表卡片（Object List）
 * <a href="https://gitee.com/qlangtech/palantir-study/blob/master/foundry/workshop/detail/04-widgets/widgets-object-list.md">...</a>
 * <p>
 * 以卡片网格形式展示对象集合，支持响应式布局和点击选择输出变量。
 * <p>
 * 字段分四组：<b>数据来源</b>（{@code objectSetVar} + 级联出的 {@code titleProperty}）、
 * <b>内容</b>（{@code cardFields}）、<b>展示形态</b>（{@code display}，列表/网格二选一）、
 * <b>交互</b>（{@code activeObjectVar} 写回、{@code cardActions} 行内动作）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/12
 */
public class ObjectListWidget extends FullDisplayWidget implements IWidgetColumnHost {

    public static final String KEY_OBJECT_SET_VAR = "objectSetVar";
    public static final String KEY_ACTIVE_OBJECT_VAR = "activeObjectVar";
    public static final String KEY_TITLE_PROPERTY = "titleProperty";
    public static final String KEY_CARD_FIELDS = "cardFields";
    public static final String KEY_DISPLAY = "display";
    public static final String KEY_CARD_ACTIONS = "cardActions";

    /** 列表数据来源：当前模块中的对象集合变量（options 由 Descriptor 动态供给） */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 10, advance = false, validate = {Validator.require})
    public String objectSetVar;

    /**
     * 卡片标题属性。
     * <p>
     * 选项由 {@code objectSetVar} 级联得出（见 Descriptor 中的 {@code valueChangePipe}），
     * 因此必须是<b>同层普通字段</b>，不能下沉进 {@code cardFields} 子表单。
     */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 11, advance = false, validate = {Validator.require})
    public String titleProperty;

    /**
     * 卡片正文展示的字段列表（由子表单结构化管理，非手写 JSON）
     */
    @FormField(type = FormFieldType.MULTI_DESCRIBLE_PLUGIN, desClazz = WidgetColumnConfig.class, ordinal = 12, advance = false, validate = {Validator.require})
    public List<WidgetColumnConfig> cardFields;

    /**
     * 展示形态：列表 / 网格。
     * <p>
     * 不指定 {@code type} —— 字段类型声明为 {@link ObjectListDisplay} 子类时，
     * TIS 自动识别为嵌套子表单（CLAUDE.md 原则一）。选 Grid 只渲染 Grid 的字段，
     * 选 List 只渲染 List 的字段；这正是当初不做「mode 枚举 + 可空载荷字段」的原因。
     * <p>
     * 默认项由同名 {@code .json} 里的 {@code "dftVal": "Grid"} 逐字匹配子类
     * {@code getDisplayName()} 决定（见 {@link ObjectListDisplay} 类注释）。
     */
    @FormField(ordinal = 13, advance = false)
    public ObjectListDisplay display;

    /** 当前选中项写回：点击卡片后写入该变量，供下游组件消费 */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 14, advance = false)
    public String activeObjectVar;

    /** 卡片行内动作按钮（点击后触发本体 Action），默认收起在「高级」分组 */
    @FormField(type = FormFieldType.MULTI_DESCRIBLE_PLUGIN, desClazz = WidgetActionConfig.class, ordinal = 15)
    public List<WidgetActionConfig> cardActions;

    @Override
    public String getBoundObjectSetVar() {
        return this.objectSetVar;
    }

    @Override
    public List<WidgetColumnConfig> getColumnConfigs() {
        return this.cardFields;
    }

    @TISExtension
    public static class DescriptorImpl extends BaseWidgetDescriptor<IWorkshopWidget> {

        public DescriptorImpl() {
            super();
            this.registerSelectOptions(KEY_OBJECT_SET_VAR, WidgetOptionHelper::getObjectSetVariableOptions);
            this.registerSelectOptions(KEY_ACTIVE_OBJECT_VAR, WidgetOptionHelper::getObjectSetVariableOptions);

            // === 级联属性字段：初始为空（未选 objectSetVar 时无选项）===
            this.registerSelectOptions(KEY_TITLE_PROPERTY, Collections::emptyList);

            // === 级联管道：objectSetVar 变化时，同时更新 titleProperty 的候选属性与 cardFields 的候选列 ===
            //
            // titleProperty 与 cardFields 同源于 objectSetVar，但需要的内容形态不同（前者是属性下拉选项，
            // 后者是列配置行），故用逐字段渲染，而不能共用一个广播结果。
            //
            // 注意两者对 FunctionConfig 型对象集的口径目前不一致：getObjectPropertyOptions 内部宽
            // catch 会静默降级为空候选，而 cardFields 走的 getObjectPropertyMetas 会直接抛错。
            // 已就此在 WidgetOptionHelper 上留了说明；本次有意不改动前者。
            this.valueChangePipe(KEY_OBJECT_SET_VAR, KEY_TITLE_PROPERTY, KEY_CARD_FIELDS)
                    .render(KEY_TITLE_PROPERTY, (pluginMeta, params) -> {
                        String selectedVar = params.getString(KEY_OBJECT_SET_VAR);
                        if (selectedVar == null) {
                            return Collections.emptyList();
                        }
                        return WidgetOptionHelper.getObjectPropertyOptions(
                                selectedVar, OntologyPluginMeta.createPluginMeta(pluginMeta));
                    })
                    .render(KEY_CARD_FIELDS, (pluginMeta, params) -> WidgetColumnConfig.createFrom(
                            WidgetOptionHelper.getObjectPropertyMetas(params.getString(KEY_OBJECT_SET_VAR))));
        }

        @Override
        public String getDisplayName() {
            return "Object List";
        }

        @Override
        public WorkShopWidgetType getWidgetType() {
            return WorkShopWidgetType.OBJECT_LIST;
        }

        @Override
        protected Map<String, Object> appendExtractProps(Map<String, Object> props) {
            props.put("defaultSize", Map.of("height", 400));
            return props;
        }
    }
}