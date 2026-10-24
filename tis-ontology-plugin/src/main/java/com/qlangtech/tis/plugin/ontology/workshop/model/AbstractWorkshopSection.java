package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.enums.SectionParentType;
import com.qlangtech.tis.plugin.ontology.workshop.model.section.SectionLayout;
import com.qlangtech.tis.plugin.ontology.workshop.model.section.SectionStyleConfig;
import com.qlangtech.tis.plugin.ontology.workshop.widget.WorkshopWidget;
import com.qlangtech.tis.util.IPluginContext;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Optional;
import java.util.function.ObjIntConsumer;
import java.util.function.ToIntFunction;

/**
 * Workshop Section 的抽象基类，承载 {@link WorkshopSection} 与 {@link SectionChild} 共享的表单字段。
 * <p>
 * 设计要点：
 * <ol>
 *   <li><b>本类不实现 {@code Describable}</b>：{@code Describable<Self>} 由两个子类各自声明
 *       （{@code WorkshopSection implements Describable<WorkshopSection>}、
 *       {@code SectionChild implements Describable<SectionChild>}）。这样既能避开递归泛型
 *       {@code AbstractWorkshopSection<T extends AbstractWorkshopSection<T>>}（TIS 全库无此先例），
 *       又能让 {@code SectionChild} 以自身为 self-type 满足
 *       {@code MultiDescribleElementDescriptor<T extends Describable<T>>} 的约束。
 *       {@code @FormField} 字段按祖先链反射收集（{@code PluginExtraProps.visitAncestorsClass}，
 *       走裸 {@code getSuperclass()} 链），不依赖本类是否 {@code Describable}。</li>
 *   <li><b>identity 上收到本基类</b>：{@link #id}（UUID）标 {@code identity=true}，本类 {@code implements IdentityName}。
 *       两个子类都继承 identity。id 由前端创建 section 时生成，后端原样持久化；
 *       <b>不提供默认值也不兜底生成</b> —— {@link #identityValue()} 在 id 为空时直接抛异常。</li>
 *   <li>ordinal 沿用 0–5，子类新增字段从 6 起，避免撞号。</li>
 * </ol>
 *
 * @see WorkshopSection
 * @see SectionChild
 */
public abstract class AbstractWorkshopSection implements Serializable, IdentityName, IPluginStore.BeforePluginSaved {

    private static final long serialVersionUID = 1L;

    /**
     * identity 字段：区块 UUID。前端创建 section 时生成（{@code ModuleStateService.createSection}
     * 用 {@code crypto.randomUUID()}），后端把它当不透明值原样持久化；
     * 事件载荷（{@code sectionId}）与前端渲染都以它寻址。表单里以 {@code readonly} 展示。
     * <p>
     * <b>不提供默认值</b>：生成方是实体的创建者，后端只透传与持久化；
     * {@link #identityValue()} 在 id 为空时直接抛异常，不兜底生成。
     * <p>
     * <b>刻意不加 {@link Validator#identity}</b>：它含「不能以数字开头」的限制，而
     * {@code crypto.randomUUID()} 生成的 UUID 有 10/16 的概率以数字开头（同
     * {@link WorkshopPage#id}）。id 是前端生成的不透明值，只需非空。
     */
    @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT,
            validate = {Validator.require})
    public String id;

    /**
     * 父容器（类型 + 主键），与前端 {@code SectionBase} 的 {@code parentId} / {@code parentType}
     * 两个字段逐项对应 —— 前端是扁平两字段，这里打包成一个值对象，避免「只填了 id 忘了类型」
     * 或反过来这种可构造出来的半残状态。
     *
     * <p>Workshop 的持久化采用「子实体独立落盘 + 子侧外键归属」：page / section / widget
     * 各自是独立的插件实例，父子关系不靠 XML 嵌套表达（{@code widgets}、{@code sections}
     * 都是 transient，见各自字段注释），而是靠本字段串起来。加载某页的全部 section =
     * 按 {@code parent.parentId == page.id && parent.parentType == PAGE} 过滤 section 存储；
     * 重建树时再用 {@link #sortOrder} 排序。
     *
     * <p>三种父类型（{@link SectionParentType}）下 {@code parentId} 的含义：
     * <ul>
     *   <li>{@link SectionParentType#PAGE} → {@link WorkshopPage#id}（<b>UUID</b>，不是 page name）</li>
     *   <li>{@link SectionParentType#OVERLAY} → {@link WorkshopOverlay#id}</li>
     *   <li>{@link SectionParentType#SECTION} → 父 {@code AbstractWorkshopSection.id}</li>
     * </ul>
     *
     * <p><b>不是表单字段</b>（没有 {@code @FormField}）：归属由服务端赋值（见
     * {@code WorkshopPage#addSection} / {@code WorkshopOverlay#addSection} /
     * {@code WorkshopSection#addChildSection}），不由用户编辑；XStream 会照常持久化，
     * 落盘形态是嵌套的 {@code <parent><parentId/><parentType/></parent>}。
     *
     * <p>子分区 {@link SectionChild} 虽然随父 section 嵌套落盘，本字段仍写父 section 的 id ——
     * 一旦将来改成独立落盘，归属语义不需要再迁移。
     */
    public WorkshopCptParentId parent;

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String name;

    /**
     * 区块布局。<b>布局身份由取值自身的 Java 类型承担</b> ——
     * {@code ColumnsLayout} / {@code RowsLayout} / {@code TabsLayout} /
     * {@code FlowLayout} / {@code ToolbarLayout} / {@code LoopLayout}。
     *
     * <p>本字段原先是一对强相关的字段：一个 {@code SectionLayout} <b>枚举</b>说"是哪种布局"，
     * 外加一个 {@code SectionLayoutConfig} 抽象类说"那种布局的参数"。两者现已合并 ——
     * 枚举与抽象类并存的形态允许自相矛盾的状态（{@code layout = COLUMNS} 配一个
     * {@code TabsLayout}），且两套分类体系已经漂移（枚举 6 个常量 / 配置类 3 个子类）。
     */
    @FormField(ordinal = 2, validate = {Validator.require})
    public SectionLayout layout;

    @FormField(ordinal = 3)
    public SectionStyleConfig styleConfig;

    /**
     * 条件可见性，与 Widget 的 {@code WidgetDisplayConfig.conditionalVisibility} 共用同一多态体系
     * （{@link ConditionalVisibilityConfig}）。默认 {@link ConditionalVisibilityOff}：区块恒显示。
     * <p>
     * 字段初始化只在「表单解析 / 新建对象」时执行；XStream 反序列化不走构造器，旧数据缺该元素时
     * 仍是 {@code null}，由 {@code WorkshopModule.loadLayout} 与前端归一化统一视为 off。
     */
    @FormField(ordinal = 4, validate = {Validator.require})
    public ConditionalVisibilityConfig conditionalVisibility = new ConditionalVisibilityOff();


    /**
     * 同一父容器内的显示顺序 —— <b>只在所属父容器内有意义</b>（page / overlay / 父 section
     * 各有一套从 0 开始的序号），不跨容器比较大小。
     * <p>
     * 移动（拖拽排序或跨容器移动）后必须重排受影响的父容器（前端 reducer 负责），
     * 服务端重建树时按本字段稳定排序即可，见 {@link #normalizeOrder}。
     */
    @FormField(ordinal = 5, type = FormFieldType.INT_NUMBER)
    public Integer sortOrder = 0;

    /**
     * 区块内的 Widget 列表。元素类型是 tis-plugin 的 Widget 基类 —— 具体是哪种 Widget
     * 由元素自身的 Java 类型承担（实例 JSON 上表现为扁平 {@code impl} 键）。
     * <p>
     * transient、不进表单：widget 仍由画布投放区添加。
     */
    public transient List<WorkshopWidget> widgets = new ArrayList<>();

    /**
     * 把 widget 挂到本 section 下，并同步维护子侧外键 {@link WorkshopWidget#sectionId} ——
     * 两条归属线（内存集合 + 落盘外键）只在这一处同时写入，避免日后新增调用点只改一边。
     *
     * @throws IllegalStateException 本 section 还没有 id（见 {@link #identityValue()}）
     */
    public void addWidget(WorkshopWidget widget) {
        if (widgets == null) {
            widgets = new ArrayList<>();
        }
        widget.sectionId = identityValue();
        widgets.add(widget);
    }

    /**
     * 同一父容器内的 widget 序号归一化：先按 {@link #sortOrder} 稳定排序，再压实成 0..n-1。
     * 只在「该父容器的 widget 集合已经完整重建」之后调用。
     * <p>
     * {@code widgets} 是 transient，XStream 反序列化后是 {@code null}（不走字段初始化），
     * 所以这里要容忍 {@code null}。
     */
    public void normalizeWidgetOrder() {
        normalizeOrder(widgets,
                w -> w.sortOrder == null ? 0 : w.sortOrder,
                (w, index) -> w.sortOrder = index);
    }

    /**
     * 同层排序 + 序号压实，供各父容器复用 —— {@code sortOrder} 的语义是
     * 「父容器内序号」，重排之后必须重新编号，否则会出现空洞 / 重号。
     *
     * @param siblings  同一父容器下的完整兄弟列表；为 {@code null} 或空时直接返回
     * @param sortOrder 读当前序号（{@code null} 视为 0）
     * @param setter    写新序号
     */
    public static <T> void normalizeOrder(List<T> siblings, ToIntFunction<T> sortOrder, ObjIntConsumer<T> setter) {
        if (siblings == null || siblings.isEmpty()) {
            return;
        }
        // List.sort 是稳定排序：sortOrder 相同的兄弟保持原有相对顺序
        siblings.sort(Comparator.comparingInt(sortOrder));
        for (int i = 0; i < siblings.size(); i++) {
            setter.accept(siblings.get(i), i);
        }
    }

    @Override
    public void beforeSaved(IPluginContext pluginContext, Optional<Context> context) {
        if (this.parent == null) {
            throw new IllegalStateException("parent can not be null");
        }
    }

    /**
     * @throws IllegalStateException id 为空 —— 说明创建方（前端建 section）没有下发 id，
     *                               属于链路上的缺陷，此处不兜底生成
     */
    @Override
    public String identityValue() {
        if (this.id == null) {
            throw new IllegalStateException(getClass().getSimpleName() + " id can not be null");
        }
        return this.id;
    }
}
