package com.qlangtech.tis.plugin.ontology.workshop.widget.impl.chartxy;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

import java.io.Serializable;

/**
 * 系列聚合方式的抽象基类。
 *
 * <p>原先是 {@code SeriesConfig} 上的 {@code AggregationType} 枚举 + 一个平铺的
 * {@code property} 字段。枚举值本身没问题，问题在于「哪些配置属于哪种聚合」表达不出来：
 * 计数不需要度量属性，但表单照旧要求填。改为多态后，{@code property} 上提到本基类
 * （六种聚合里五种都要它），计数子类在自己的 {@code .json} 里把它 {@code disable} 掉。
 *
 * <h3>为什么 property 在基类而不在每个子类</h3>
 * 需求明确：property 属于聚合方式的公共载荷，而不是某一子类的专有字段。放在基类还带来
 * 一个好处 —— 新增一种「需要度量属性的聚合」时不必再抄一遍字段；不需要它的子类（当前只有
 * Count）用 json 覆盖即可。
 *
 * <h3>为什么 property 不能挂 Validator.require</h3>
 * 校验发生在<b>抽象基类的字段声明</b>上，{@code disable} 只影响表单渲染，不影响校验遍历。
 * 若挂 require，Count 子类的默认配置会永远校验失败。因此「非 Count 必须有 property」这条
 * 约束在 {@code ChartXYDataProcessor} 执行取数时做运行期校验。
 *
 * <h3>为什么必须有 .json</h3>
 * 本类虽然是抽象的，但它声明了 {@code @FormField}。资源加载按类名查找，
 * {@code AbstractSeriesAggregation.json} 会被各子类的 descriptor 作为祖先合并进来，
 * 子类只需覆盖差异项（见 {@link CountAggregation}）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/24
 */
public abstract class AbstractSeriesAggregation
        implements Describable<AbstractSeriesAggregation>, Serializable {

    private static final long serialVersionUID = 1L;

    public static final String KEY_PROPERTY = "property";

    /**
     * 参与聚合的度量属性名。计数不使用本字段（子类的 json 会把它从表单移除）。
     *
     * <p>候选由宿主 {@code ChartLayer} 的级联管道供给：
     * {@code valueChangePipe("dataSourceVar", "series[].aggregation.property")} —— 三段路径
     * （列表字段 {@code []} → 多态子表单字段 → 其子属性），前端
     * {@code item-prop-val.component.ts#propValChange} 一路下钻改写每个 impl 的候选。
     *
     * <p>必须 {@code SELECTABLE} 而不是 {@code INPUTTEXT}：管道只改写候选，改成自由文本就等于
     * 让级联结果没有地方落地。也不该是 {@code ENUM}：候选是运行时数据（本体属性清单），
     * 不是固定枚举。
     *
     * <p>{@code BasicDescriptor} 里登记了候选来源兜底（空列表）—— SELECTABLE 字段未注册会让
     * {@code DescriptorsJSON#getSelectOptions} 抛 {@code has not been register}，整张表单打不开。
     */
    @FormField(ordinal = 0, type = FormFieldType.SELECTABLE, validate = Validator.require)
    public String property;

    public abstract static class BasicDescriptor extends Descriptor<AbstractSeriesAggregation> {

        public BasicDescriptor() {
            super();
            // 兜底注册：真实候选由 ChartLayer 的级联管道在渲染期回填。
            // 挂在抽象基类上，六种聚合子类的 descriptor 自动继承，不必逐个抄。
            this.registerSelectOptions(KEY_PROPERTY, java.util.Collections::emptyList);
        }
    }
}
