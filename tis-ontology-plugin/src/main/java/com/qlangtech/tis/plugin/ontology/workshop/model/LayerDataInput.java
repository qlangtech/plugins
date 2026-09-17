package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;

import java.io.Serializable;

/**
 * 图层的数据输入来源（抽象基类）
 *
 * <p>本类描述 {@link ChartLayer} 的「数据从哪来」，与「画成什么形状」
 * （{@link ChartLayer#layerType}）、「按哪个属性分组」（{@link ChartLayer#xAxisProperty}）
 * 正交。基类<b>不承载任何字段</b> —— 目前只有一个子类，且它与后续可能的输入方式之间
 * 尚无已确认的公共字段。宁可留空，也不把未经验证属「公共」的字段提到基类
 * （提到基类意味着所有输入方式都适用，这是需要证据的断言）。
 *
 * <h3>为什么删掉 dataInputType 枚举</h3>
 * 旧实现是 {@code ChartLayer} 上的扁平组合 —— {@code dataInputType}（枚举）+
 * {@code dataSourceVar}（SELECTABLE）。两个失败模式：
 * <ul>
 *   <li><b>该枚举零读者</b>：全工程没有任何一处按 {@code dataInputType} 分支，
 *       表单却照样把它渲染成三选一</li>
 *   <li><b>选项供给与判别符脱钩</b>：{@code ChartLayer.DescriptorImpl} 无条件注册
 *       对象集变量选项，于是「时间序列集」也列对象集变量 —— 三个选项里两个通不了</li>
 * </ul>
 * 多态把「<b>选项从哪来</b>」交给具体子类：各子类在自己的 {@code Descriptor} 里注册
 * 自己的 {@code registerSelectOptions}。这是本类当前<b>唯一经证实</b>的收益 ——
 * 字段集的分叉尚未证实，见下。形态对齐本模块既有先例 {@link AxisConfig}、
 * {@code VariableDefinitionConfig}、{@code OverlayTypeConfig}。
 *
 * <h3>为什么只有 OBJECT_SET 一个子类</h3>
 * 被删除的枚举有三个值（{@code OBJECT_SET} / {@code FUNCTION_AGGREGATION} /
 * {@code TIME_SERIES_SET}），但<b>不能 1:1 映射成三个子类</b> —— 那三个值并不在同一条
 * 轴上：
 * <ul>
 *   <li>{@code OBJECT_SET} 与 {@code TIME_SERIES_SET} 是
 *       {@link com.qlangtech.tis.plugin.ontology.workshop.enums.VariableType} 的两个取值
 *       —— 它们描述数据的<b>形状</b></li>
 *   <li>{@code FUNCTION_AGGREGATION} <b>不是</b> VariableType 的任何取值</li>
 * </ul>
 * {@code VariableType} 里没有「函数聚合」，是因为函数算出的变量，其类型就是函数返回值的
 * 类型（NUMERIC / STRING …）。「函数聚合」在本模块是 {@code ObjectSetAggregationConfig}
 * 与 {@code FunctionConfig} —— 二者均为 {@code VariableDefinitionConfig} 的子类，描述的是
 * 「这个变量的值<b>怎么算出来</b>」。把它当作本类的兄弟子类，等于把「值的算法」这条轴
 * 混进「数据的形状」这条轴（同类错误见 {@code ContinuousAxisConfig.AxisScale} 的
 * LINEAR/LOG/TIME —— TIME 是轴属性的数据类型，不是刻度类型）。
 *
 * <p>时间序列集则<b>暂不建子类</b>，且理由与「没接线」无关 —— 是它<b>未必构成一次多态</b>：
 * 现有先例 {@code TimelineWidget} 同时接受对象集与时间序列集，用的仍是<b>同一个字段</b>
 * （一个 SELECTABLE 的 {@code dataSourceVar}），只把 options 取两类的并集
 * （{@code WidgetOptionHelper.getVariableOptions(OBJECT_SET, TIME_SERIES_SET)}），
 * 没有任何额外字段。
 *
 * <h3>新增子类的门槛</h3>
 * 不是「执行链路接线了」，而是<b>字段集确实分叉了</b>。当前唯一待验证的分叉点：时间序列
 * 图层的 X 轴是否需要自带一个时间范围（{@code TimeSeriesVisualization} 有
 * {@code timeRange}，{@code TimelineWidget} 没有，两者不一致）。据此二选一：
 * <ul>
 *   <li><b>需要</b> → 建 {@code TimeSeriesSetDataInput}（{@code dataSourceVar} + 时间范围），
 *       本多态成立</li>
 *   <li><b>不需要</b> → 它不该是子类。应改为「同一个 {@code dataSourceVar} 字段 + 声明
 *       本图层接受哪几种 {@code VariableType}」，options 按声明过滤，并删除本类</li>
 * </ul>
 * 若将来建了时间序列子类，记得补一条跨维度约束：<b>时间序列集输入仅支持折线</b>
 * （{@link ChartLayer.LayerType#LINE}）。它横跨「数据来源」与「画法」两个维度，
 * 多态编码不了，得靠 validator 或渲染侧兜住。
 *
 * <p>另：{@code ChartXYWidget} 的 javadoc 引用了设计文档
 * {@code detail-design/workshop/05-08-widget-implementation-chartXY-widget.md}，
 * 但该文档在本仓库中<b>未找到</b>，未能核对原设计对三种输入方式的字段约定。
 * 若能找到，应以该文档为准。
 *
 * <h3>字段序位</h3>
 * 基类（当前无字段）占 {@code 0..5}，各子类字段从 {@code 10} 起编排，避免 ordinal 撞车
 * —— TIS 的表单排序是对 {@code formField.ordinal()} 做稳定排序，而字段集合来自
 * HashMap，相同 ordinal 的相对顺序不确定。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/15
 * @see ObjectSetDataInput
 */
public abstract class LayerDataInput implements Describable<LayerDataInput>, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 数据输入 descriptor 基类。
     *
     * <p>可见性为 {@code public}：测试与宿主侧需要引用各子类 descriptor 以做扫描/断言，
     * protected 跨包不可见。形态对齐 {@link AxisConfig.BasicDescriptor}。
     */
    public abstract static class BasicDescriptor extends Descriptor<LayerDataInput> {
    }
}