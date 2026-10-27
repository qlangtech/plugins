package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.AbstractAxisTitleConfig;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.AbstractNumericFormatting;
import com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy.AxisTitleOff;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 坐标轴配置（抽象基类）
 *
 * <p>本类只承载<b>两种轴性质共有</b>的显示配置；轴性质专有的配置分别落在
 * {@link CategoricalAxisConfig}（分类轴，额外有排序方式）与 {@link ContinuousAxisConfig}
 * （连续轴，额外有刻度类型与上下界）。
 *
 * <h3>为什么是多态而不是「一个类 + kind 枚举」</h3>
 * 分类轴与连续轴的专有字段互不适用：分类轴没有「刻度类型」，连续轴没有「排序方式」。
 * 若压成单类加 {@code kind} 字段，两组字段会同时出现在表单里，用户必须自行判断哪些
 * 与自己选的轴性质有关。多态则让 TIS 原生的 impl 选择器先定轴性质、再渲染对应字段，
 * 形态对齐本模块既有先例 {@code VariableDefinitionConfig} 与 {@code OverlayTypeConfig}。
 *
 * <h3>轴绑定<b>不在</b>本类</h3>
 * 「轴上画什么」由 {@link ChartLayer#xAxisProperty} 与各 series 的聚合方式决定；
 * 本类只描述轴<b>长什么样</b>。原先 {@code AxisConfig} 上的 {@code variable}
 * （必填 INPUTTEXT）把数据来源混进了显示配置，已删除。
 *
 * <h3>字段序位</h3>
 * 基类占用 {@code 0..5}，各子类字段从 {@code 10} 起编排，避免 ordinal 撞车
 * —— TIS 的表单排序是对 {@code formField.ordinal()} 做稳定排序，而字段集合来自
 * HashMap，相同 ordinal 的相对顺序不确定。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/15
 * @see CategoricalAxisConfig
 * @see ContinuousAxisConfig
 */
public abstract class AxisConfig implements Describable<AxisConfig>, Serializable {

    private static final long serialVersionUID = 1L;

    // ordinal 1 / 2 留空：原 titleOverride 已下沉到 showTitle 的 on 子类，
    // 原 enableNumericalFormatting 已由 numericFormatting 的 off 子类取代
    // （按本包既有约定，删字段后序位留空、不重排）。

    /**
     * 轴标题配置（off / 显示两态多态）。
     *
     * <p>「显示」子类自带 {@code titleOverride} 且必填 —— 开关与载荷是同一个对象，
     * 不再可能出现「关了却还填着标题」的落盘状态。
     */
    @FormField(ordinal = 0, validate = {Validator.require})
    public AbstractAxisTitleConfig showTitle = new AxisTitleOff();

    /**
     * 数值格式化配置（off / 启用两态多态）；默认启用。
     */
    @FormField(ordinal = 3, validate = {Validator.require})
    public AbstractNumericFormatting numericFormatting = new NumericFormatting();

    /**
     * 是否显示网格线。
     */
    @FormField(ordinal = 4, type = FormFieldType.ENUM)
    public Boolean showGridlines = false;

    /**
     * 是否显示颜色标记（轴上的系列配色圆点）。
     */
    @FormField(ordinal = 5,type = FormFieldType.ENUM)
    public Boolean showColorMarkers = false;

    /**
     * {@link com.qlangtech.tis.plugin.ontology.workshop.widget.impl.ChartXYWidget#xAxis} 的
     * 可选实现过滤器 —— <b>只保留分类轴</b>。
     *
     * <p>在 {@code ChartXYWidget.json} 里以 {@code xAxis.subDescEnumFilter} 声明，由
     * {@code PropertyType#applicableDescriptors} 用 Groovy 编译执行，结果决定该字段的 impl
     * 下拉里出现哪些选项。这是把「X 轴只能是分类轴」表达在<b>表单层</b>的第一道闸：用户在下拉里
     * 就看不到连续轴，而不是配完再被保存校验驳回。
     *
     * <h4>它<b>不是</b>校验，不能替代 verify</h4>
     * 过滤器只影响下发给前端的候选集。{@code AttrValMap#createDescribable} 走
     * {@code TIS.get().getDescriptor(impl)} 解析 impl，<b>不会回查本过滤器</b> ——
     * 直接 POST 一个 {@code ContinuousAxisConfig} 的 impl 依然能通过解析。因此保存期的
     * {@code ChartXYWidget#axisKindViolation()} 与运行期的同类检查一个都不能少。
     *
     * <h4>为什么判据写 {@code desc.clazz} 而不是 {@code instanceof}</h4>
     * 过滤器的入参声明类型是 {@code List<? extends Descriptor>}（TIS 的扩展点是
     * {@code Descriptor} 而非具体 descriptor 类型），所以元素静态类型上无法安全地
     * {@code instanceof CategoricalAxisConfig.DefaultDescriptor}；{@code desc.clazz} 是
     * {@code Descriptor} 的公共字段（无参构造器取 enclosing class），正是「这个 descriptor
     * 描述哪个类」的权威来源。
     *
     * <p>字段名必须叫 {@code desc}：{@code PropertyType#applicableDescriptors} 生成的
     * Groovy 方法签名是 {@code apply(List<? extends Descriptor> desc)}，脚本体直接引用该形参。
     *
     * @param descs 该扩展点的全部 descriptor，可能为 {@code null}
     * @return 分类轴的 descriptor；入参为 {@code null} 时返回空列表而非 {@code null}
     */
    public static List<? extends Descriptor> categoricalOnly(List<? extends Descriptor> descs) {
        if (descs == null) {
            return Collections.emptyList();
        }
        return descs.stream() //
                .filter(desc -> CategoricalAxisConfig.class.isAssignableFrom(desc.clazz)) //
                .collect(Collectors.toList());
    }

    /**
     * {@link com.qlangtech.tis.plugin.ontology.workshop.widget.impl.ChartXYWidget#yAxis} 的
     * 可选实现过滤器 —— <b>只保留连续轴</b>。
     *
     * <p>与 {@link #categoricalOnly} 成对；约定、局限与字段命名要求见该方法的 javadoc。
     *
     * @param descs 该扩展点的全部 descriptor，可能为 {@code null}
     * @return 连续轴的 descriptor；入参为 {@code null} 时返回空列表而非 {@code null}
     */
    public static List<? extends Descriptor> continuousOnly(List<? extends Descriptor> descs) {
        if (descs == null) {
            return Collections.emptyList();
        }
        return descs.stream() //
                .filter(desc -> ContinuousAxisConfig.class.isAssignableFrom(desc.clazz)) //
                .collect(Collectors.toList());
    }

    /**
     * XStream 反序列化不走构造器与字段初值，空值回填由宿主
     * {@code ChartXYWidget.readResolve()} 调用本方法完成。
     *
     * <p>本类无法声明私有的 {@code readResolve()}：XStream 不会把父类的 readResolve
     * 应用到子类实例上，而 {@link CategoricalAxisConfig} / {@link ContinuousAxisConfig}
     * 才是真正被实例化的类型。故用公开方法 + 宿主统一调用。
     */
    public void normalize() {
        if (this.showTitle == null) {
            this.showTitle = new AxisTitleOff();
        }
        if (this.numericFormatting == null) {
            this.numericFormatting = new NumericFormatting();
        }
        // 连续轴的上下界同样要兜：本方法是宿主统一入口，两个子类在这里分流成本最低
        // （子类各自声明 readResolve 不会被 XStream 应用到父类方法解析上，见上面的说明）。
        if (this instanceof ContinuousAxisConfig continuous) {
            continuous.normalizeBounds();
        }
    }

    /**
     * 轴配置 descriptor 基类。
     *
     * <p>可见性为 {@code public}：{@code ChartXYWidget} 的表单侧需要引用各子类
     * descriptor，protected 跨包不可见。形态对齐 TIS 中同类先例
     * {@code ValueConstraint.BaseDesc} 与本模块的 {@code VariableDefinitionConfig.BasicDescriptor}。
     */
    public abstract static class BasicDescriptor extends Descriptor<AxisConfig> {
    }
}
