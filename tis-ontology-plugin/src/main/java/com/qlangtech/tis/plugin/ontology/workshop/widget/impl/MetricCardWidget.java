package com.qlangtech.tis.plugin.ontology.workshop.widget.impl;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.model.CardLayout;
import com.qlangtech.tis.plugin.ontology.workshop.model.LayoutStyle;
import com.qlangtech.tis.plugin.ontology.workshop.model.MetricConfig;
import com.qlangtech.tis.plugin.ontology.workshop.widget.FullDisplayWidget;
import com.qlangtech.tis.plugin.workshop.widget.IWorkshopWidget;

import java.util.ArrayList;
import java.util.List;

/**
 * P0 Widget：指标卡（Metric Card）
 * <p>
 * 一张卡片里可以放<b>多个</b>指标（Palantir 的 Metric Card 本就是多指标容器：一排 KPI 卡片、
 * 一组标签、一个指标列表），因此指标配置是 {@code List<MetricConfig>} 而不是单个对象。
 * 每个指标的标签、取值变量、数值格式、条件着色、副指标等全部由 {@link MetricConfig} 承载，
 * 指标之间的呈现方式由 {@link LayoutStyle} 的三种子类决定：
 * <ul>
 *   <li>{@link com.qlangtech.tis.plugin.ontology.workshop.model.CardLayout} —— 每个指标一张卡片，可配方向与模板</li>
 *   <li>{@link com.qlangtech.tis.plugin.ontology.workshop.model.TagLayout} —— 紧凑标签，可配方向</li>
 *   <li>{@link com.qlangtech.tis.plugin.ontology.workshop.model.ListLayout} —— 上下列表，可配模板</li>
 * </ul>
 * <p>
 * 为什么布局用多态而不是「一个枚举 + direction / template 两个可为空的字段」：
 * 后者能表达出「选了标签布局却还配着 template」这种自相矛盾的状态，且表单里永远露出
 * 与当前布局无关的字段；多态把参数收进各自的子类，表单只展示本布局真正需要的项。
 *
 * <a href="https://gitee.com/qlangtech/palantir-study/blob/master/foundry/workshop/detail/04-widgets/widgets-metric-card.md">...</a>
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/12
 */
public class MetricCardWidget extends FullDisplayWidget {

    /**
     * 指标列表。行序即显示序（{@link MetricConfig} 的 {@code order} 字段支持拖拽调整）。
     */
    @FormField(ordinal = 10, type = FormFieldType.MULTI_DESCRIBLE_PLUGIN, desClazz = MetricConfig.class,
            validate = {Validator.require})
    public List<MetricConfig> metrics = new ArrayList<>();

    /**
     * 指标之间的布局样式（多态，见类注释）。默认卡片布局，与 Palantir 的默认值一致。
     */
    @FormField(ordinal = 11, validate = {Validator.require})
    public LayoutStyle layoutStyle = new CardLayout();

    /**
     * 数值字号档位。仅影响数值文本的大小，不影响布局。
     */
    @FormField(ordinal = 12, type = FormFieldType.ENUM)
    public MetricSize size = MetricSize.REGULAR;

    /**
     * XStream 反序列化老版本 XML（当时只有单个 {@code metric} 字段）时，
     * 列表/布局字段都会是 null，前台渲染会 NPE，这里补默认值。
     */
    private Object readResolve() {
        if (this.metrics == null) {
            this.metrics = new ArrayList<>();
        }
        if (this.layoutStyle == null) {
            this.layoutStyle = new CardLayout();
        }
        if (this.size == null) {
            this.size = MetricSize.REGULAR;
        }
        return this;
    }

    /**
     * 数值字号档位。
     * <p>
     * 实现 {@link DescriptorUseableShortComment} 后，选项由服务端反射枚举常量生成
     * （label = 常量名，help = {@code shortComment()}），json 资源里不再声明 {@code enum}。
     */
    public enum MetricSize implements DescriptorUseableShortComment {
        COMPACT("紧凑"),
        REGULAR("标准"),
        LARGE("大号");

        private final String comment;

        MetricSize(String comment) {
            this.comment = comment;
        }

        @Override
        public String shortComment() {
            return this.comment;
        }
    }

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
