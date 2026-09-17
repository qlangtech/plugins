package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.widget.FilterItemConfig;
import com.qlangtech.tis.plugin.ontology.workshop.widget.WorkshopWidget;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;

import java.util.List;

/**
 * P0 Widget：过滤列表（Filter List）
 * <p>
 * 组合多种过滤器组件（关键词、单选、多选、日期选择），统一输出过滤条件。
 *
 * <h3>为什么需要 {@link #objectSetVar}</h3>
 * 本 Widget 有两个用途不同的对象集概念，缺一不可：
 * <ul>
 *   <li>{@code objectSetVar} 决定<b>过滤谁</b>——单选/多选控件的候选项要按该对象集的
 *       属性枚举约束在运行时推导（见 {@code filter-list.component.ts}），
 *       写出的谓词也要带上它的 {@code objectType} 供下游判定能否施加；</li>
 *   <li>{@link #filterOutputVar} 决定<b>写去哪</b>——一个 OBJECT_SET_FILTER 类型变量，
 *       由下游的对象集变量通过 {@code ObjectSetDefinitionConfig.filterVar} 引用。</li>
 * </ul>
 * 二者在页面上是独立可变的：同一个过滤变量可以被多条链路复用。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/12
 */
public class FilterListWidget extends WorkshopWidget {

    public static final String KEY_OBJECT_SET_VAR = "objectSetVar";
    public static final String KEY_FILTERS = "filters";
    public static final String KEY_FILTER_OUTPUT_VAR = "filterOutputVar";

    /**
     * 过滤作用的对象集变量：决定候选项来源与写出谓词的 objectType。
     */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 20, advance = false, validate = {Validator.require})
    public String objectSetVar;

    /**
     * 过滤项列表。
     *
     * <p>非必填：空列表表示「不过滤」，是页面刚建好、过滤项还没配时的合法中间态
     * （{@link FilterItemElementCreatorFactory} 对 null 行数组同样按空列表处理）。
     */
    @FormField(type = FormFieldType.MULTI_SELECTABLE, ordinal = 21, advance = false)
    public List<FilterItemConfig> filters;

    /**
     * 过滤结果写回：组合出的过滤条件写入该「对象集合过滤器」变量，供下游组件作为过滤源
     */
    @FormField(type = FormFieldType.SELECTABLE, ordinal = 22, advance = false)
    public String filterOutputVar;

    @TISExtension
    public static class DescriptorImpl extends BaseWidgetDescriptor<IWorkshopWidget> {

        public DescriptorImpl() {
            super();
            this.registerSelectOptions(KEY_OBJECT_SET_VAR, WidgetOptionHelper::getObjectSetVariableOptions);
            this.registerSelectOptions(KEY_FILTER_OUTPUT_VAR,
                    WidgetOptionHelper::getObjectSetFilterVariableOptions);
        }

        @Override
        public String getDisplayName() {
            return "Filter List";
        }

        @Override
        public WorkShopWidgetType getWidgetType() {
            return WorkShopWidgetType.FILTER_LIST;
        }
    }
}
