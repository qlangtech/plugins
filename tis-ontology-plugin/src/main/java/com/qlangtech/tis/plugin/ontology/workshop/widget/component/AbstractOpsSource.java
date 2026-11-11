package com.qlangtech.tis.plugin.ontology.workshop.widget.component;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.util.DescribableJSON;

import java.io.Serializable;
import java.util.List;
import java.util.Map;

/**
 * 候选项来源 —— 单选 / 多选控件「可选项从哪来」的抽象基类。
 * {@link ManualOpsSource} / {@link ObjectSetOpsSource} 二选一。
 *
 * <h3>为什么是独立的多态族，而不是挂在 {@link AbstractSelectComponent} 上的几个可空字段</h3>
 * 「手动列表」与「取自对象集变量」的载荷完全不同：前者是一张 val/label 子表格，后者是
 * 三个下拉（变量 + label 属性 + val 属性）外加一条级联管道。扁平化后必然出现
 * 「选了 manual 却还填着 sourceObjectSetVar」这类自相矛盾的状态，而框架只有静态
 * {@code advance} 分组、没有条件可见性。多态把「带哪些载荷」交给子类 ——
 * 与 {@link FilterComponent}、{@code SizingMode} 同一处理方式。
 *
 * <h3>为什么 {@link BasicDescriptor} 独立于 {@link FilterComponent.BasicDescriptor}</h3>
 * {@code DescriptorExtensionList} 按 {@code describableType.isAssignableFrom(descriptor.getT())}
 * 筛选。两个族共用一个 {@code Descriptor<FilterComponent>} 会让「候选项来源」的两个实现
 * 出现在「控件形态」下拉里（反之亦然）—— 编译期不报错，只是下拉里凭空多出几项。
 *
 * <h3>为什么本类没有 {@code optionsSource} 之外的公共字段</h3>
 * 两个子类连「候选项列表」这个最有共性的东西都不共享：{@link ObjectSetOpsSource} 不落盘
 * 任何候选项，它在取数时实时查库。把 {@code List<WidgetOptionConfig>} 提到本类，就等于
 * 逼着 object-set 这一支也写一份必然过期的副本。
 *
 * <h3>候选项为什么走 {@link DescribableJSON.AppendPayloadAttribute}</h3>
 * 候选项<b>不是落盘配置</b>，而是读时现算的快照（object-set 那一支甚至要查一次库），
 * 放进 {@code vals} 会破坏「{@code vals} 就是可回传配置」这条语义。本类把「怎么取」交给
 * 多态子类（{@link #createOptions}），把「怎么下发」收口到 {@link #createPayloadProps()}：
 * <ul>
 *   <li>{@link #createPayloadProps()} 是<b>唯一</b>序列化点，外层键名由
 *       {@link Descriptor#KEY_OPTIONS} 决定，行格式由 {@link Option#toJson(List)} 决定
 *       （{@code {label, val}}）；</li>
 *   <li>{@link #createOptions} 是<b>唯一</b>取数点。外部（曾经的 {@code FilterOptionResolver}）
 *       不再按 {@code instanceof} 分发 —— 新增第三种来源时是编译期强制，而不是运行期静默空列表；</li>
 *   <li>载荷随 module GET 在 {@code appendVals.options} 下一次下发；配置面板编辑过之后
 *       （前端 {@code Item.project()} 会把 {@code appendVals} 丢掉）由
 *       {@code FilterListWidget} 的 {@code get_options} 端点再序列化一次 ——
 *       两条链路走的是<b>同一段</b> {@code getItemJson()}，故键名不可能分叉。</li>
 * </ul>
 *
 * <p>⚠️ 候选项<b>不落盘</b>、也不由前端攒请求体：两条链路的服务端都用 {@code DescribableJSON}
 * 序列化整份 widget，前端只读响应里的 {@code appendVals.options}。
 * 由此带来的时效性：候选项是**读时快照**，面板里的编辑要到下一次 module GET 或补发的
 * {@code get_options} 才会重算。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/2
 * @see AbstractSelectComponent#optionsSource
 * @see DescribableJSON.AppendPayloadAttribute
 */
public abstract class AbstractOpsSource implements Describable<AbstractOpsSource>, Serializable, DescribableJSON.AppendPayloadAttribute {

    private static final long serialVersionUID = 1L;

    /**
     * 下发形态：{@code {"options": [{label, val}, ...]}}，由 {@code getItemJson()} 挂在
     * item 信封的 {@code appendVals} 键上（与 {@code vals} 同级）。
     *
     * <p>对 {@code createOptions()} 返回 null 做兜底：{@code Map.of} 遇 null 值直接 NPE，
     * 而这里抛出去会让整个 module 的序列化失败（见 {@link DescribableJSON.AppendPayloadAttribute}
     * 的实现契约）。
     */
    @Override
    public final Map<String, Object> createPayloadProps(List<Describable<?>> parent) {
        List<Option> options = createOptions();
        return Map.of(Descriptor.KEY_OPTIONS, Option.toJson(options == null ? List.of() : options));
    }

    /**
     * 本来源的可选值 —— 各子类唯一的取数点。
     *
     * <p><b>契约：永不返回 null、永不抛异常。</b>调用点在序列化链路里（GET 与 POST 各一次），
     * 一次异常会中止整个 module / 页面的下发与保存。失败请在本方法内部降级成空列表并记 warn ——
     * 「取不到候选项」最坏只是控件里没有选项，绝不能让 Widget 取数或保存失败。
     *
     * @return 候选项；没有可选项时返回空列表
     */
    protected abstract List<Option> createOptions();

    public abstract static class BasicDescriptor extends Descriptor<AbstractOpsSource> {
    }
}
