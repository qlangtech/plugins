package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;

/**
 * 按时间粒度分桶（Temporal Bucketing）
 *
 * <p>适用于 X 轴属性为<b>日期或时间戳</b>的图层。两个字段合起来表达一个时间刻度：
 * <b>粒度</b>（切到哪一级）与<b>粒度单位数</b>（一个刻度跨几级）。
 *
 * <pre>
 *   granularity=DAY,   unitValue=1  →  每天一个刻度        （byDays）
 *   granularity=DAY,   unitValue=7  →  每 7 天一个刻度     （byDays(7)）
 *   granularity=HOUR,  unitValue=6  →  每 6 小时一个刻度   （byHours(6)）
 * </pre>
 *
 * <p>两个字段必须成对存在：只有粒度没有单位数，表达不了「双周」「季度汇总」这类最常见的
 * 业务刻度；只有单位数没有粒度，连单位是什么都不知道。这正是它们同属一个子类、
 * 且不该被压成「一个粒度枚举塞满 周/双周/月/季度/年」的原因——后者会让枚举值随组合爆炸。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/15
 * @see XAxisBucketing
 */
public class TemporalBucketing extends XAxisBucketing {

    private static final long serialVersionUID = 1L;

    /**
     * 时间粒度 —— 刻度切到哪一级；默认「天」。
     */
    @FormField(ordinal = 10, type = FormFieldType.ENUM, validate = {Validator.require})
    public TimeGranularity granularity = TimeGranularity.DAY;

    /**
     * 粒度单位数 —— 一个刻度包含几个粒度单位；默认 1（即每 1 个粒度一个刻度）。
     */
    @FormField(ordinal = 11, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer unitValue = 1;

    /**
     * 时间粒度。
     *
     * <p>取值集合对齐对象集聚合 API 的分桶方法：{@code byYear / byQuarter / byMonth / byWeek /
     * byDays / byHours / byMinutes / bySeconds}。其中时 / 分 / 秒三级仅<b>时间戳</b>属性支持，
     * 纯日期属性最多切到天——这是属性类型的约束，多态编码不了，由使用侧兜住。
     *
     * <p>{@link #shortComment()} 在此处只作为选项的<b>说明文案</b>下发；选项的显示 label
     * 来自 {@code TemporalBucketing.json} 的 {@code enum} 数组，因为 TIS 的
     * {@code resolveEnumLabel} 对枚举一律返回常量名。
     */
    public enum TimeGranularity implements DescriptorUseableShortComment {
        YEAR("年"),
        QUARTER("季度"),
        MONTH("月"),
        WEEK("周"),
        DAY("天"),
        HOUR("小时（仅时间戳属性）"),
        MINUTE("分钟（仅时间戳属性）"),
        SECOND("秒（仅时间戳属性）");

        private final String comment;

        TimeGranularity(String comment) {
            this.comment = comment;
        }

        @Override
        public String shortComment() {
            return comment;
        }
    }

    @TISExtension
    public static class DefaultDescriptor extends BasicDescriptor implements DescriptorUseableShortComment {

        @Override
        public String getDisplayName() {
            return "Temporal Bucketing";
        }

        @Override
        public String shortComment() {
            return "按时间粒度分桶";
        }

        // validateLimit（桶数上限）由 BasicDescriptor 提供，此处无需重声明 —— 见那里的说明。

        /**
         * 粒度单位数必须为正整数：0 会让每个刻度宽度为零，负值无意义。
         */
        public boolean validateUnitValue(IFieldErrorHandler msgHandler, Context context,
                                         String fieldName, String value) {
            int val = Integer.parseInt(value);
            if (val < 1) {
                msgHandler.addFieldError(context, fieldName, "必须大于 0");
                return false;
            }
            return true;
        }
    }
}
