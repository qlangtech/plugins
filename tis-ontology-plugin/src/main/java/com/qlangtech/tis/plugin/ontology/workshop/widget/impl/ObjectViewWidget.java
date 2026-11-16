package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.widget.WidgetColumnConfig;
import com.qlangtech.tis.plugin.ontology.workshop.widget.WorkshopWidget;
import com.qlangtech.tis.plugin.ontology.workshop.widget.IWidgetColumnHost;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;

import java.util.List;

/**
 * P0 Widget：对象详情视图（Object View）
 * <p>
 * 以描述列表（nz-descriptions）形式展示单个对象的属性详情。分组标题用基类的 {@code title}。
 *
 * <h3>两个对象变量各有分工，不要合并</h3>
 * <table>
 *   <tr><th>字段</th><th>作用</th><th>能选什么</th></tr>
 *   <tr><td>{@link #objectVar}</td>
 *       <td><b>属性来源</b>：只用于级联生成 {@link #properties} 的候选行</td>
 *       <td>只有「对象集合定义」型变量有 schema（{@code ObjectSetVariable#getColsMeta()}）</td></tr>
 *   <tr><td>{@link #activeObjectVar}</td>
 *       <td><b>运行时展示对象</b>：组件订阅它的当前值</td>
 *       <td>任意 OBJECT_SET，含「外部写入」型 —— 那正是接收行选中项的典型用法</td></tr>
 * </table>
 *
 * <p>运行时取值<b>优先 {@code activeObjectVar}、回退 {@code objectVar}</b>。回退是为了兼容存量
 * 手工配置（早先只有一个字段，用户把可查询的对象集变量填在 {@code objectVar} 上）；
 * 新增配置两者通常都填，且分别指向「可查询的对象集」与「外部写入的选中项载体」。
 *
 * <p>典型链路（详见 {@code ObjectListWidget#onActiveObjectSelection}）：
 * ObjectList 行选中 → 写 {@code activeObjectVar} → 本 Widget 订阅到新对象 → 重绘；
 * 若同时配了 {@code open-overlay} 事件，浮层里的 ObjectView 实例化后首发即拿到该对象
 * （浮层只按名字寻址、没有参数通道，共享变量是唯一的传导方式）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/12
 */
public class ObjectViewWidget extends WorkshopWidget implements IWidgetColumnHost {

    public static final String KEY_OBJECT_VAR = "objectVar";
    public static final String KEY_ACTIVE_OBJECT_VAR = "activeObjectVar";
    public static final String KEY_PROPERTIES = "properties";

    /**
     * 属性来源：当前模块中的对象集合变量，只用于决定 {@link #properties} 的候选行。
     * <p>
     * 不加 {@link Validator#require}：运行时取值可以先走 {@link #activeObjectVar}，
     * 而「只配 activeObjectVar、属性全手填」是一种合法配置。
     */
    @FormField(type = FormFieldType.SELECTABLE,ordinal = 10)
    public String objectVar;

    /** nz-descriptions 的列数 */
    @FormField(type = FormFieldType.INT_NUMBER,ordinal = 11,validate = {Validator.integer})
    public Integer columnCount;

    /**
     * 要展示的属性列表，由子表单结构化管理。
     * <p>
     * 与 {@link PropertyListWidget#properties} 是同一个模型；前端读的字段名统一按
     * {@link WidgetColumnConfig#prop} 来，ObjectView 组件此前读的是 {@code prop.key}，
     * 取不到值只能渲染空白行。
     */
    @FormField(type = FormFieldType.MULTI_DESCRIBLE_PLUGIN,desClazz = WidgetColumnConfig.class,ordinal = 12)
    public List<WidgetColumnConfig> properties;

    /**
     * 运行时展示对象：通常指向 ObjectList / ObjectTable 的 activeObjectVar
     * （「外部写入」型 OBJECT_SET 变量），行选中后自动刷新。
     * <p>
     * 不加 {@link Validator#require}：存量 widget 只有 {@code objectVar}，加了会让所有老配置保存失败。
     */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 13)
    public String activeObjectVar;

    @Override
    public String getBoundObjectSetVar() {
        // 只服务于「列/行候选从哪个对象集取 schema」（IWidgetColumnHost 契约），
        // 因此永远是属性来源 objectVar，不读 activeObjectVar —— 后者多半是没有 schema 的外部写入型。
        return this.objectVar;
    }

    @Override
    public List<WidgetColumnConfig> getColumnConfigs() {
        return this.properties;
    }

    @TISExtension
    public static class DescriptorImpl extends BaseWidgetDescriptor<IWorkshopWidget> {

        public DescriptorImpl() {
            super();
            this.registerSelectOptions(KEY_OBJECT_VAR, WidgetOptionHelper.getObjectSetVariableOptions());
            // 运行时对象可以是任意 OBJECT_SET（含「外部写入」型），与 ObjectList 的 activeObjectVar
            // 用同一个供货方 —— 那两个字段指的是同一批变量。用 getObjectSetSourceVariableOptions
            // 会把唯一该选的类型（外部写入）排除掉。
            this.registerSelectOptions(KEY_ACTIVE_OBJECT_VAR, WidgetOptionHelper.getObjectSetVariableOptions());

            // === 级联管道：objectVar 变化时，按该对象集变量的属性自动生成候选属性行 ===
            this.valueChangePipe(KEY_OBJECT_VAR, KEY_PROPERTIES).render((pluginMeta, param) ->
                    WidgetColumnConfig.createFrom(
                            WidgetOptionHelper.getObjectPropertyMetas(param.getString(KEY_OBJECT_VAR))));
        }

        @Override
        public String getDisplayName() {
            return "Object View";
        }

        @Override
        public WorkShopWidgetType getWidgetType() {
            return WorkShopWidgetType.OBJECT_VIEW;
        }
    }
}