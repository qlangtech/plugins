package com.qlangtech.tis.plugin.ontology.workshop.model.config;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.WidgetOptionHelper;

/**
 * Workshop 基于变量的可见性配置（默认实现）：引用一个模块变量，按<b>比较表达式</b>
 * 控制宿主浮层（{@code WorkshopOverlay}）的显示隐藏。
 *
 * <p>多态体系见父类 {@link VariableBasedVisibilityConfig}：「不启用」由零字段的子类
 * {@link NoneVariableBasedVisibility} 表达；本类只在用户显式选择启用时出现，因此
 * {@link #variableId} 与 {@link #expression} 都带 {@code Validator.require}。
 *
 * <p><b>本类只存配置，不做求值</b>。改造前这里有一个 {@code isVisible(Object)}
 * 方法，全仓无任何调用方（前端 {@code workshop-overlay.component.ts} 里对应的分支也只是
 * 注释占位），故已删除；真正的显隐求值链路（事件 / 布局服务）尚未接线，
 * 届时按 {@link #expression} + {@link #compareValue} 实现即可。
 *
 * <p>与之相对，同项目 {@code ConditionalVisibility} 只判变量真假、不比较；
 * 两者能力不同，尚未收敛。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/13
 */
public class VariableBasedVisibility extends VariableBasedVisibilityConfig {

    private static final long serialVersionUID = 1L;

    public static final String KEY_VARIABLE_ID = "variableId";
    public static final String KEY_EXPRESSION = "expression";
    public static final String KEY_COMPARE_VALUE = "compareValue";

    /**
     * 被引用的模块变量名（大小写不敏感）。
     *
     * <p>字段名是历史遗留：{@code WorkshopVariable} 的 id 已删除、identity 字段就是 name。
     * 之所以不把字段名一并改成 variableName —— 它同时是表单的线格式键：
     * 本类经 {@code WorkshopOverlay.variableBasedVisibility}（{@code @FormField(ordinal = 7)}）
     * 作为嵌套表单下发，前端 {@code overlay.model.ts} 的 {@code variableBasedVisibility.variableId}
     * 与 {@code section.model.ts} 的 {@code ConditionalVisibility.variableId} 都按这个键读取，
     * 改名会静默断开这条链路。要改必须两端同批。
     *
     * <p>候选变量由本类自己的 descriptor 注册（同 {@code ConditionalVisibility}），
     * 宿主无需各自再注册一遍。
     */
    @FormField(ordinal = 0, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String variableId;

    /**
     * 比较表达式。取值固定，硬编码为 {@link VisibilityExpression}（CLAUDE.md 原则二）。
     *
     * <p>改造前这里是裸 {@code String}，拼错只能静默落到 {@code default: return true}；
     * 换成枚举后非法取值在表单层即被拦住。
     */
    @FormField(ordinal = 1, type = FormFieldType.ENUM, validate = {Validator.require})
    public VisibilityExpression expression = VisibilityExpression.equals;

    /**
     * 比较值。只有比较型算子（{@code equals} / {@code greaterThan} / {@code lessThan}）需要；
     * {@code notEmpty} / {@code truthy} 忽略此值 —— 故不设 {@code Validator.require}。
     */
    @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT)
    public String compareValue;

    /**
     * 可见性比较表达式。
     *
     * <p><b>常量名必须逐字等于改造前持久化的字符串</b>（{@code equals} / {@code notEmpty} /
     * {@code greaterThan} / {@code lessThan} / {@code truthy}）：XStream 按 {@code name()} 落盘与读回，
     * 换成大写会读不回老数据；且不实现 {@code IEnumTypeSer} 时表单选项的 value 就是
     * {@code name()}，前端线格式也因此零变化。
     *
     * <p>中文标签走 {@link #shortComment()}，由 {@code OptionWithEndType#createEnumOptions}
     * 派生为选项 help —— 因此 {@code .json} 里<b>不写</b> {@code enum} 数组。
     */
    public enum VisibilityExpression implements DescriptorUseableShortComment {
        equals("等于比较值（字符串相等）"),
        notEmpty("非空：字符串非空串，其余类型恒真"),
        greaterThan("大于比较值（按数字比较）"),
        lessThan("小于比较值（按数字比较）"),
        truthy("为真：布尔 true / 数字非 0 / 其余类型恒真");

        private final String comment;

        VisibilityExpression(String comment) {
            this.comment = comment;
        }

        @Override
        public String shortComment() {
            return comment;
        }
    }

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

        public DefaultDescriptor() {
            super();
            this.registerSelectOptions(KEY_VARIABLE_ID, WidgetOptionHelper::getWorkshopVariableOptions);
        }

        @Override
        public String getDisplayName() {
            return "Variable Based Visibility";
        }

        @Override
        public String shortComment() {
            return "按变量比较表达式控制浮层显隐";
        }
    }
}
