package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.widget.ButtonColor;

import java.io.Serializable;
import java.util.List;

/**
 * 条件格式化规则。
 *
 * <p>一条规则 = 「匹配条件 + 命中后套用的样式」。规则按列表顺序求值，<b>首条命中生效</b>；
 * {@link MatchMode#ALWAYS} 用作兜底（Palantir 的 otherwise 规则），不参与比较、恒命中。
 *
 * <h3>为什么 {@code name} 必须是 identity</h3>
 * identity 值会作为子表单行的主键被下发（{@code DescriptorsJSON} 的 {@code pkField}）、
 * 并在落盘/回填时用于定位行。不能拿 {@code operand} 顶替：
 * {@code Validator.identity} 的字符集是 {@code [A-Z\d a-z_\-]+}，
 * {@code "<= 0"} 这样的比较值含空格与比较符，会被直接拒掉。
 *
 * <h3>颜色为什么是 {@link ButtonColor} 而不是十六进制字符串</h3>
 * 文本色 / 背景色 / 趋势线色与按钮颜色是同一套品牌色语义，复用现成的多态体系后：
 * 固定色（primary/success/warning/danger）由前端统一映射，只有「自定义」子类才带
 * {@code customColor} 载荷并由 {@code validateCustomColor} 做十六进制校验 —— 免去在本类
 * 里再写一遍颜色校验，也不会出现「选了预设色还填着十六进制」的自相矛盾状态。
 * {@code none} 表示<b>不改变</b>该处颜色。
 *
 * <h3>图标为什么是 {@link IEndTypeGetter.EndType}</h3>
 * 该枚举的 {@code val} 同时是合法的 Ant Design 图标名（{@code IEnumTypeSer} 让表单线与
 * 存储都用 {@code val} 而不是常量名），前端 {@code <i nz-icon [nzType]>} 可直接渲染；
 * 写成自由文本的话，拼错只会静默渲染成空图标。
 *
 * @author 百岁 (baisui@qlangtech.com)
 */
public class ConditionalFormatRule implements Describable<ConditionalFormatRule>, Serializable,
        IPluginStore.MultiDescribleElement {

    private static final long serialVersionUID = 1L;

    /** 规则名，仅用于在列表中区分各条规则（同时是子表单行的 identity） */
    @FormField(identity = true,ordinal = 0,type = FormFieldType.INPUTTEXT,validate = {Validator.require})
    public String name;

    @FormField(ordinal = 1, type = FormFieldType.ENUM)
    public MatchMode matchMode = MatchMode.STANDARD;

    @FormField(ordinal = 2, type = FormFieldType.ENUM)
    public Comparator comparator = Comparator.LESS_THAN_OR_EQUALS;

    /**
     * 单值比较的操作数（数字或字符串）。
     * <p>
     * 刻意存成 String 而不是 Double：同一个字段要同时承载数值与字符串比较，
     * 而字符串指标（如「当前模式：实盘」）也要能配规则。类型转换在前端求值时按
     * {@link MetricConfig.ValueType} 决定。
     */
    @FormField(ordinal = 3, type = FormFieldType.INPUTTEXT)
    public String operand;

    /**
     * 区间下界，仅 {@link Comparator#BETWEEN} / {@link Comparator#NOT_BETWEEN} 使用。
     * 空串或 {@code any} 表示无下界。
     */
    @FormField(ordinal = 4, type = FormFieldType.INPUTTEXT)
    public String lowerBound;

    /**
     * 区间上界，仅 {@link Comparator#BETWEEN} / {@link Comparator#NOT_BETWEEN} 使用。
     * 空串或 {@code any} 表示无上界（即 Palantir 的「between 1,000 and any」）。
     */
    @FormField(ordinal = 5, type = FormFieldType.INPUTTEXT)
    public String upperBound;

    /** 文本颜色；{@code none} = 不改变 */
    @FormField(ordinal = 6)
    public ButtonColor color;

    /** 背景颜色；{@code none} = 不改变 */
    @FormField(ordinal = 7)
    public ButtonColor backgroundColor;

    /**
     * 趋势线（sparkline）颜色；{@code none} = 不改变。
     * <p>
     * 仅当卡片布局启用了时间序列可视化（{@link CardLayout#visualization}）时才有渲染对象。
     */
    @FormField(ordinal = 8)
    public ButtonColor sparklineColor;

    /**
     * 命中后显示在数值旁的图标；留空 = 不显示。
     * <p>
     * json 里用 {@code enumMode: model_assist}：该枚举常量近 800 条，走弹出式搜索面板，
     * 避免把全部图标塞进表单首屏 JSON。
     */
    @FormField(ordinal = 9,type = FormFieldType.ENUM)
    public IEndTypeGetter.EndType icon;

    /**
     * 规则优先级。子表单支持拖拽排序（见描述符的 {@code isEnableRowDragDrop}），
     * 前端读取时按本字段升序排序后求值。
     */
    @FormField(ordinal = 10,type = FormFieldType.INT_NUMBER)
    public Integer order = 0;

    @Override
    public String identityValue() {
        return this.name;
    }

    /**
     * 匹配方式。
     * <p>
     * 实现 {@link DescriptorUseableShortComment} 后，选项由服务端反射枚举常量生成
     * （label = 常量名，help = {@code shortComment()}），json 资源里不再声明 {@code enum}。
     */
    public enum MatchMode implements DescriptorUseableShortComment {
        /** 按 {@link #comparator} / 操作数比较 */
        STANDARD("按条件匹配"),
        /** 兜底规则：恒命中，通常放在列表末位 */
        ALWAYS("始终匹配");

        private final String comment;

        MatchMode(String comment) {
            this.comment = comment;
        }

        @Override
        public String shortComment() {
            return this.comment;
        }
    }

    /**
     * 比较符。
     * <p>
     * 数值比较符按数值大小理解；字符串比较符仅在 {@link MetricConfig.ValueType#STRING} 下有意义。
     * {@code IS_NULL} / {@code IS_NOT_NULL} 不使用操作数。
     */
    public enum Comparator implements DescriptorUseableShortComment {
        EQUALS("等于"),
        NOT_EQUALS("不等于"),
        LESS_THAN("小于"),
        LESS_THAN_OR_EQUALS("小于等于"),
        GREATER_THAN("大于"),
        GREATER_THAN_OR_EQUALS("大于等于"),
        BETWEEN("介于区间"),
        NOT_BETWEEN("不介于区间"),
        CONTAINS("包含"),
        NOT_CONTAINS("不包含"),
        STARTS_WITH("开头是"),
        ENDS_WITH("结尾是"),
        IS_NULL("为空"),
        IS_NOT_NULL("不为空");

        private final String comment;

        Comparator(String comment) {
            this.comment = comment;
        }

        @Override
        public String shortComment() {
            return this.comment;
        }
    }

    @TISExtension
    public static class DefaultDescriptor extends IPluginStore.MultiDescribleElementDescriptor<ConditionalFormatRule> {

        @Override
        public String getDisplayName() {
            return "Conditional Format Rule";
        }

        @Override
        public String shortComment() {
            return "条件格式规则";
        }

        @Override
        public ViewStyle viewStyle() {
            return ViewStyle.Table;
        }

        @Override
        public List<ColConfig> colsConfig() {
            // width 是百分比（见前端 MultiChildViewDescriptor.colWidth），三列合计 100
            return Lists.newArrayList(
                    new ColConfig("name", 30).setClickable(),
                    new ColConfig("comparator", 40),
                    new ColConfig("operand", 30));
        }

        @Override
        protected RowDragDropCfg isEnableRowDragDrop() {
            // 规则顺序即优先级（首条命中），必须允许拖拽重排
            return new RowDragDropCfg(true, "order");
        }
    }
}
