package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.aggregation.TimeGranularity;
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

    // TimeGranularity 已提升到
    // com.qlangtech.tis.plugin.ontology.aggregation.TimeGranularity：
    // 核心层的聚合 API（GroupKey）也要表达「按哪一级粒度切桶」，而核心层不该依赖本插件
    // 工程的表单模型。常量名与 shortComment 原样保留，XStream / 表单选项按常量名持久化，
    // 存量配置不受影响。时 / 分 / 秒仅时间戳属性的约束由使用侧（聚合下推 planner）校验。

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
