package com.qlangtech.tis.plugin.ontology.workshop.widget.component;

import com.qlangtech.tis.plugin.annotation.FormField;

/**
 * <b>单选 / 多选</b>控件的公共父类：在 {@link FilterComponent} 之上多一个
 * {@link #optionsSource}（候选项从哪来）。
 *
 * <h3>为什么这一层要单独存在</h3>
 * 需求是「{@code SingleSelectComponent} 与 {@code MultiSelectComponent} 隶属于同一个父类，
 * 父类里放 {@code optionsSource}」。若把 {@code optionsSource} 提到 {@link FilterComponent} 上，
 * 关键词与日期选择器也会凭空出现一个「候选项来源」子表单 —— 那是无意义的必填项；
 * 若在两个子类里各写一份，字段声明重复且 ordinal 容易撞号。所以中间插一层：
 * 有选项的两个子类继承它，没有选项的两个直接继承 {@link FilterComponent}。
 *
 * <h3>为什么本类要有自己的 {@code .json}</h3>
 * 本类是抽象的，但它声明了 {@code @FormField}。资源按类名逐层合并，
 * {@code AbstractSelectComponent.json} 会被两个子类的 descriptor 当作祖先读入，
 * 子类不需要（也不应该）各写一份。这也是 {@code TestWorkshopFormDefaults}
 * 「有字段的抽象基类必须亲手写 json」那条守卫生效的地方 —— 它的自动扫描只看具体类。
 *
 * <h3>为什么本类不挂 {@code @TISExtension}</h3>
 * sezpoz 索引不过滤抽象类：注解若写在抽象基类上，抽象类型会混进「控件形态」下拉。
 * 规则是「只有最终能被用户选中的具体实现才带注解」——
 * {@link SingleSelectComponent} / {@link MultiSelectComponent} 各自的嵌套 descriptor 带注解。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/2
 * @see SingleSelectComponent
 * @see MultiSelectComponent
 */
public abstract class AbstractSelectComponent extends FilterComponent {

    private static final long serialVersionUID = 1L;

    public static final String KEY_OPTIONS_SOURCE = "optionsSource";

    /**
     * 候选项来源。
     *
     * <p>字段类型声明为抽象的 {@link AbstractOpsSource}（<b>不</b>写 {@code type}、<b>不</b>写
     * {@code desClazz}）：{@code PropertyType.isDescribable()} 为真时表单会把它渲染成嵌套的
     * 多态子表单，可选项来自 {@code TIS.get().getDescriptorList(AbstractOpsSource.class)}。
     * 一旦写成 {@code desClazz} 或 {@code type = ENUM}，子表单机制就退化成单值控件。
     *
     * <p>Java 初值 {@link ObjectSetOpsSource} 与 {@code AbstractSelectComponent.json} 里的
     * {@code "dftVal": "object-set"} 是<b>一对</b>：前端新建表单的初值只认 {@code eprops.dftVal}
     * （{@code tis.plugin.ts} 的 {@code addNewEmptyItemProp}），Java 初始化器它一概不读；
     * 而 {@code TestWorkshopFormDefaults} 又要求两者逐字一致。改其中一个必须同时改另一个。
     */
    @FormField(ordinal = 4)
    public AbstractOpsSource optionsSource = new ObjectSetOpsSource();

    /**
     * XStream 反序列化不跑构造器、也不跑字段初始化器：存量配置可能整段缺失
     * {@code optionsSource}（本字段存在之前的落盘数据），这里补成默认来源。
     *
     * <p>必须声明在<b>本类</b>：{@code readResolve} 只对自己的实例生效，不会沿继承链下发到
     * 子类实例 —— 写在 {@link FilterComponent} 上对 {@link SingleSelectComponent} 无效。
     */
    private Object readResolve() {
        this.normalize();
        return this;
    }

    /**
     * 把缺失的 {@code optionsSource} 补成默认值；幂等。
     */
    public void normalize() {
        if (this.optionsSource == null) {
            this.optionsSource = new ObjectSetOpsSource();
        }
    }

    /**
     * 单选 / 多选两支共享的 descriptor 基类。
     *
     * <p>extends {@link FilterComponent.BasicDescriptor} 而不是自己造一个
     * {@code Descriptor<AbstractSelectComponent>}：这两个子类是通过「控件形态」下拉被选中的，
     * 必须留在 {@link FilterComponent} 的 {@code DescriptorExtensionList} 里。
     *
     * <p><b>不带</b> {@code @TISExtension}：本类没有对应的具体插件，带注解会让一个
     * 「实现类是抽象类」的 descriptor 进选择器。
     */
    public abstract static class SelectDescriptor extends FilterComponent.BasicDescriptor {
    }
}
