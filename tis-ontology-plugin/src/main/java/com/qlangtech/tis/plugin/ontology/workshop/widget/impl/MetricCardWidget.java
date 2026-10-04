package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.model.MetricConfig;
import com.qlangtech.tis.plugin.ontology.workshop.widget.FullDisplayWidget;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;

/**
 * P0 Widget：指标卡（Metric Card）
 * <p>
 * 以统计卡片形式展示单个数值指标。
 * <p>
 * 指标的标题、取值变量、数值格式、条件着色、副指标、迷你趋势图全部由 {@link MetricConfig}
 * 承载 —— 原先这里只有平铺的一个 {@code valueVar}，而前端读的却是
 * {@code widget.title}/{@code prefix}/{@code suffix}/{@code color}，两边对不上，
 * 卡片永远是空白的。改为嵌套 MetricConfig 后，指标这种「多字段一体」的概念只有一处定义，
 * 指标卡与其它用到指标的组件共享同一份配置。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/12
 */
public class MetricCardWidget extends FullDisplayWidget {

    public static final String KEY_METRIC = "metric";

    @FormField(ordinal = 10, advance = false, validate = {Validator.require})
    public MetricConfig metric;

    /**
     * 副指标（如同比、环比）。
     * <p>
     * 与 {@link #metric} <b>平级</b>，不是 {@code MetricConfig} 的字段 —— 主副指标在
     * Palantir 的 Metric Card 里本来就是兄弟关系，且自嵌套的 Describable 会让表单序列化
     * 无限递归后爆栈，理由见 {@link MetricConfig} 的类注释。
     */
    @FormField(ordinal = 11, advance = false)
    public MetricConfig secondaryMetric;

    @FormField(ordinal = 12, advance = false, type = FormFieldType.ENUM)
    public Boolean showSecondaryMetric = false;

    @TISExtension
    public static class DescriptorImpl extends BaseWidgetDescriptor<IWorkshopWidget> {

        @Override
        public String getDisplayName() {
            return "Metric Card";
        }

        @Override
        public WorkShopWidgetType getWidgetType() {
            return WorkShopWidgetType.METRIC_CARD;
        }
    }
}