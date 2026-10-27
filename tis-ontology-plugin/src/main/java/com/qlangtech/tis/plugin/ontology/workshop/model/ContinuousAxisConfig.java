package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.AbstractDecimalBound;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.DecimalBoundConfig;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.DecimalBoundOff;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;

/**
 * 连续轴配置（Continuous Axis / Value Axis）
 *
 * <p>轴上绘制的是<b>连续数值</b>（聚合结果），因此需要刻度类型与上下界。
 * 与之相对的是 {@link CategoricalAxisConfig}。
 *
 * <h3>关于 {@code TIME} 刻度</h3>
 * 旧实现把 {@code LINEAR / LOG / TIME} 并列成「刻度类型」，这是类别错误：
 * <b>时间不是刻度类型，而是轴属性的数据类型</b>。时间轴由
 * {@link ChartLayer#xAxisProperty} 指向 timestamp 属性 + 运行期时间桶粒度决定，
 * 与刻度类型正交。因此本枚举只保留官方文档承认的线性与对数两种。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/15
 */
public class ContinuousAxisConfig extends AxisConfig {

    private static final long serialVersionUID = 1L;

    /**
     * 刻度类型；默认线性。
     */
    @FormField(ordinal = 10, type = FormFieldType.ENUM)
    public AxisScale scaleType = AxisScale.LINEAR;

    /**
     * 是否按 series 分别配置值轴。
     *
     * <p>仅在同一图层/图表存在多个 series、且各 series 数量级差异悬殊时才有意义。
     */
    @FormField(ordinal = 11,type = FormFieldType.ENUM)
    public Boolean useMultipleAxes = false;

    // ordinal 13 / 15 留空：原 minBound / maxBound 两个裸 Double 已并入下方多态字段
    // （按本包既有约定，删字段后序位留空、不重排）。

    /**
     * 下界（自动计算 / 手工指定两态多态，见 {@link AbstractDecimalBound}）；默认自动。
     *
     * <p>改造前这里是 {@code autoMinBound}（布尔）+ {@code minBound}（Double）两个平铺字段：
     * 「自动」开着时手工值照样能填，且不生效 —— 用户填了却看不到效果。多态化后手工值只存在于
     * {@link DecimalBoundConfig} 里，矛盾状态无法落盘。
     */
    @FormField(ordinal = 12, validate = {Validator.require})
    public AbstractDecimalBound minBound = new DecimalBoundOff();

    /**
     * 上界（自动计算 / 手工指定两态多态，见 {@link AbstractDecimalBound}）；默认自动。
     *
     * @see #minBound
     */
    @FormField(ordinal = 14, validate = {Validator.require})
    public AbstractDecimalBound maxBound = new DecimalBoundOff();

    /**
     * 上下界的空值回填，由宿主 {@link AxisConfig#normalize()} 分流调用。
     *
     * <p>缺了它，多态化之前落盘的连续轴（XML 里只有 {@code autoMinBound} / {@code minBound}
     * 平铺元素，没有 {@code minBound} 的 impl 子元素）反序列化后两个字段都是 {@code null}：
     * 表单渲染 NPE，且前端拿不到「自动」这一态。回填成 {@link DecimalBoundOff}，与改造前的
     * 默认（{@code auto*=true}）一致。
     *
     * <p>被丢弃的旧值：那些 {@code autoMinBound=false + minBound=12.5} 的历史实例，其手工值
     * 在旧 XML 里是平铺元素，新类没有同名字段可接（严格转换器会抛
     * {@code UnknownFieldException}）—— 见 {@link #minBound} 的说明，本次改造按「未发布数据」处理。
     */
    void normalizeBounds() {
        if (this.minBound == null) {
            this.minBound = new DecimalBoundOff();
        }
        if (this.maxBound == null) {
            this.maxBound = new DecimalBoundOff();
        }
    }

    /**
     * 刻度类型。仅线性与对数两种 —— 时间轴不是刻度类型，见类注释。
     */
    public enum AxisScale implements DescriptorUseableShortComment {
        LINEAR("线性（默认）"),
        LOG("对数");

        private final String comment;

        AxisScale(String comment) {
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
            return "Continuous Axis";
        }

        @Override
        public String shortComment() {
            return "连续轴";
        }

        /**
         * 上下界的跨字段校验：{@code maxBound} 必须严格大于 {@code minBound}。
         *
         * <p>两层字段各自都是合法的（{@code DecimalBoundConfig.value} 有
         * {@code Validator.require}，任意小数都能过），只有把两个值放在一起才谈得上大小关系，
         * 因此只能落在宿主的 {@code validateAll} 里 —— 这是「必填项等基础校验通过之后」执行，
         * {@code newInstance()} 拿得到已解析的两个多态实例。
         *
         * <p>只有<b>两侧都选了「手工指定」</b>时才比较：任一侧为 {@link DecimalBoundOff}
         * 表示该侧由图表实际值自动推断，此时不存在可比较的确定值（自动算出来的下界本来就可能
         * 高于用户手工填的上界，那是另一回事，不该在保存期拦截）。
         *
         * <p>相等也判为非法：上下界相等会得到一个零高度的轴，用户几乎不可能是这个意图。
         */
        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            ContinuousAxisConfig axis = postFormVals.newInstance();
            Double min = axis.minBound instanceof DecimalBoundConfig minCfg ? minCfg.value : null;
            Double max = axis.maxBound instanceof DecimalBoundConfig maxCfg ? maxCfg.value : null;
            if (min != null && max != null && max <= min) {
                msgHandler.addFieldError(context, "maxBound",
                        "上界必须大于下界（当前下界=" + min + "，上界=" + max + "）");
                return false;
            }
            return true;
        }
    }
}
