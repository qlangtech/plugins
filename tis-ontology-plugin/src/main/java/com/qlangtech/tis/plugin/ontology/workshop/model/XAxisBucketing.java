package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;

import java.io.Serializable;

/**
 * X 轴分桶方式（抽象基类）
 *
 * <h3>分桶是什么，为什么属于图层</h3>
 * {@link ChartLayer#xAxisProperty} 只回答「按<b>谁</b>分组」，本类回答「分组键怎么<b>切</b>」。
 * 同一个属性，切成「天」还是「小时」得到的是两张完全不同的图：前者几十个点，
 * 后者可能几十万个点。因此分桶不是轴的显示配置（轴只管长什么样），而是数据到图形的
 * 映射方式，与 {@link ChartLayer#xAxisProperty} 同属图层。
 *
 * <h3>为什么是多态而不是「一个类 + kind 枚举」</h3>
 * 分桶方式由 X 轴属性的<b>数据类型</b>决定，且各方式的参数互不适用：
 * <ul>
 *   <li>时间戳 / 日期属性 —— 按粒度切（年 / 季 / 月 / 周 / 天 / 时 / 分 / 秒），
 *       且每个刻度可以含多个粒度单位（每 7 天一个刻度）→ {@link TemporalBucketing}</li>
 *   <li>字符串 / 布尔属性 —— 按值切（前 N 个高频值、或精确去重值）→ {@link CategoricalBucketing}</li>
 * </ul>
 * 若压成单类加 {@code kind} 字段，两组字段会同时出现在表单里——分类轴上会多出「时间粒度」，
 * 时间轴上会多出「取值方式」，用户必须自行判断哪些与自己选的属性类型有关。多态则让 TIS
 * 原生的 impl 选择器先定分桶方式、再渲染对应字段。形态对齐本模块既有先例
 * {@link AxisConfig} 与 {@link LayerDataInput}。
 *
 * <h3>为什么 limit 能提到基类</h3>
 * 与 {@link LayerDataInput} 那个「宁可留空」的基类不同，本基类承载 {@link #limit} 是<b>有证据</b>的：
 * 每一种分桶都产出桶，且都受同一个硬上限约束（对象集聚合 API 对返回的聚合结果封顶
 * {@code 10000} 个桶，超出直接抛错）。把上限放在基类，等于声明「任何分桶方式都必须
 * 自证不超过这条线」，而不是让每个子类各自记得加。
 *
 * <h3>为什么没有数值轴子类</h3>
 * 数值属性的分桶是 {@code byRanges()}（自定义区间列表）与 {@code byFixedWidth()}（固定宽度），
 * 它需要一层<b>嵌套的区间列表子表单</b>，本质上是一次独立的多态展开。本轮未采纳——
 * 与 time series 输入同理，缺失的是字段分叉的确切形态，不是接线。将来若要补，本多态
 * 直接新增 {@code NumericBucketing} 即可，基类无需改动。
 *
 * <h3>字段序位</h3>
 * 基类占用 {@code 0..5} 与 {@code 99}，各子类字段从 {@code 10} 起编排，避免 ordinal 撞车
 * —— TIS 的表单排序是对 {@code formField.ordinal()} 做稳定排序，而字段集合来自
 * HashMap，相同 ordinal 的相对顺序不确定。
 *
 * <p>{@link #limit} 取 {@code 99} 而非常规的 {@code 0..5}：它是**兜底项**，排在每个子表单
 * 的最后。若按 {@code 0} 排，基类字段会稳定地排在子类字段（{@code 10}/{@code 11}）之前，
 * 界面上「桶数上限」就跑到「时间粒度」上面去，而调完粒度才谈得上限制桶数。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/15
 * @see TemporalBucketing
 * @see CategoricalBucketing
 */
public abstract class XAxisBucketing implements Describable<XAxisBucketing>, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 桶数上限的硬上限。
     *
     * <p>对标对象集聚合 API：单次聚合返回的桶总数被封顶在 10000，超出直接抛错；
     * 其中「前 N 个高频值」这条路更严，只考虑前 1000 个值。本常量是那道天花板的本地化投影。
     */
    public static final int MAX_LIMIT = 10000;

    /**
     * 桶数上限 —— X 轴最多保留多少个桶（时间轴）/ 多少个类别值（分类轴）。
     *
     * <p>默认取满 {@link #MAX_LIMIT}：默认值不该悄悄替使用者丢数据，超限应由聚合侧报错
     * 让它显式调整。分类分桶把这个默认值下调为 1000，因为「前 N 个高频值」这条路的实际
     * 天花板就是 1000 —— 下调做在 {@link CategoricalBucketing} 的构造函数里，见那里的说明。
     *
     * <p>序位取 {@code 99}（而非基类常规的 {@code 0..5}）：它是兜底项，排在子表单最后。
     */
    @FormField(ordinal = 99, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer limit = MAX_LIMIT;

    /**
     * 分桶方式 descriptor 基类。
     *
     * <p>可见性为 {@code public}：各子类 descriptor 需在跨包的表单侧被引用，
     * protected 跨包不可见。形态对齐 {@link AxisConfig.BasicDescriptor}。
     */
    public abstract static class BasicDescriptor extends Descriptor<XAxisBucketing> {

        /**
         * {@link XAxisBucketing#limit} 的取值范围校验，两个子类共用。
         *
         * <p>声明在基类即可：{@code Descriptor.createValidateMap()} 遍历的是
         * {@code this.getClass().getMethods()}，而 {@code getMethods()} 包含<b>继承来的
         * public 方法</b>，故子类 descriptor 能查到本方法。无需在每个子类里各写一行转发。
         */
        public boolean validateLimit(IFieldErrorHandler msgHandler, Context context,
                                     String fieldName, String value) {
            int val = Integer.parseInt(value);
            if (val < 1 || val > MAX_LIMIT) {
                msgHandler.addFieldError(context, fieldName, "必须在 1 至 " + MAX_LIMIT + " 之间");
                return false;
            }
            return true;
        }
    }
}
