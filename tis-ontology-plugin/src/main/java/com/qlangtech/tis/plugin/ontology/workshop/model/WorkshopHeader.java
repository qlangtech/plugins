package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.model.header.CollapseConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.header.HeaderOrientation;
import com.qlangtech.tis.plugin.ontology.workshop.model.header.HorizontalOrientation;
import com.qlangtech.tis.plugin.ontology.workshop.model.header.NoneCollapseConfig;
import com.qlangtech.tis.util.DescribableJSON;

import java.io.Serializable;

/**
 * Workshop Header 配置。
 *
 * <h3>持久化</h3>
 * header 与 module 是 <b>1:1</b> 关系：一个模块只有一个 header。但它与
 * {@code WorkshopModule.pages/variables} 一样由独立存储落盘
 * （{@link com.qlangtech.tis.plugin.ontology.workshop.store.WorkshopHeaderStore}），
 * 而不是随模块 XML 走 —— 因为 {@code WorkshopModule.header} 是 transient 字段，
 * XStream 序列化时会跳过它。
 *
 * <p>本类不是插件、没有多实现，{@link DefaultDescriptor} 的存在只是为了
 * {@code IPluginStore} 能按 {@code impl} 反序列化落盘内容。
 *
 * <h3>两个多态字段</h3>
 * {@link #orientation} 与 {@link #collapseConfig} 都是聚合属性插件（取值集合固定、
 * 每种取值有各自的载荷），由<b>子类类型</b>而非「枚举 + 可空字段」承担。改造前它们是
 * 「一个枚举 + height/width 两个平铺字段」和「collapsible + collapsedByDefault +
 * collapsedImage 三个平铺字段」，能构造出「垂直方向 + 高度值」「不可折叠 + 折叠图标」
 * 这类自相矛盾的状态。详见两个抽象基类的类注释。
 *
 * @see com.qlangtech.tis.plugin.ontology.workshop.model.header.HeaderOrientation
 * @see com.qlangtech.tis.plugin.ontology.workshop.model.header.CollapseConfig
 */
public final class WorkshopHeader implements Describable<WorkshopHeader>, Serializable, IdentityName {

    /**
     * {@code header-update} 端点上承载整份 header 实例的请求参数名，
     * 值为 {@code {impl, vals}} 形态的 JSON 串（同 {@code WorkshopVariable.PARAM_VARIABLE}）。
     */
    public static final String PARAM_HEADER = "header";

    /**
     * 唯一标识，{@code @FormField(identity = true)} 用于 {@code IPluginStore.setPlugins()}
     * 区分实例（同 {@link WorkshopPage#id}）。值恒等于所属模块的 name —— header 是 1:1 关系，
     * 没有独立命名的必要，由 {@code WorkshopHeaderStore} 按 moduleName 定位。
     */
    @FormField(identity = true, ordinal = -1, type = FormFieldType.INPUTTEXT)
    public String id;

    /**
     * 是否在运行态渲染 header。
     *
     * <p><b>false ≠ 模块没有 header</b>。header 是恒存在的布局实体，本字段只控制它是否
     * 渲染出来；隐藏之后它仍然必须能在编辑器的 Layout 面板里被选中、配置并重新打开。
     * 对标 Palantir（concepts-layouts.md）："If the module header is hidden, it may be
     * selected with the Header option that appears at the top of the list found in the
     * Layout sidebar panel."
     *
     * <p>因此：后端 {@code WorkshopHeaderStore.loadOrDefault()} 永远返回非 null，
     * 前端导航树里 Header 节点的显示条件是「header 存在」（{@code if (module.header)}），
     * <b>不能</b>写成 {@code module.header?.visible}，否则一旦隐藏就再也点不回去了。
     */
    @FormField(ordinal = 0, type = FormFieldType.ENUM)
    public Boolean visible = true;

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT)
    public String title;

    @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT)
    public String titleColor;

    /**
     * 排布方向：水平顶栏 / 垂直侧栏。高度、宽度分别是两个子类的载荷字段，不在本类上。
     *
     * <p>聚合属性字段<b>不能</b>声明 {@code type} —— 框架按字段类型是否实现
     * {@link Describable} 自动识别为嵌套子表单并生成 impl 选择器，多写一个
     * {@code type = FormFieldType.SELECTABLE} 会在运行期报错。
     */
    @FormField(ordinal = 3, validate = {Validator.require})
    public HeaderOrientation orientation;

    /**
     * 折叠配置：不可折叠 / 可折叠（可折叠时才有默认折叠与折叠图标）。
     *
     * <p>挂在顶层而非塞进 {@link com.qlangtech.tis.plugin.ontology.workshop.model.header.VerticalOrientation}：
     * 折叠与方向是两个正交维度，水平顶栏同样可以折叠。
     */
    @FormField(ordinal = 4, validate = {Validator.require})
    public CollapseConfig collapseConfig;

    @FormField(ordinal = 5, type = FormFieldType.INPUTTEXT)
    public String backgroundColor;

    @FormField(ordinal = 6, type = FormFieldType.ENUM)
    public Boolean favoriteEnabled = true;

    public static WorkshopHeader loadOrDefault(String ontologyDomain, String moduleName) {
        return null;
    }

    /**
     * {@link IdentityName} 要求的实现，返回值与 {@link #id} 相同（模块名）—— header 与 module
     * 是 1:1 关系，没有第二个命名维度。
     *
     * <p>{@link IdentityName} 不是可选项：{@code Descriptor.getPropertyTypes()} 对「声明了
     * {@code identity = true} 字段却未实现本接口」的类<b>直接抛 IllegalStateException</b>。
     * 这点在改造前被掩盖着 —— 那时 {@link #toJSON} 是手写字段清单、根本不经过
     * {@code Descriptor.getPropertyTypes()}，所以从没人发现本类漏了这个接口；
     * 一旦改成 {@code DescribableJSON.getItemJson()} 就会立刻炸。
     */
    @Override
    public String identityValue() {
        return this.id;
    }

    /**
     * 兜底默认值：存储里还没有配置时使用。
     * <p>
     * 与 Palantir 的行为对齐 —— 新建模块的 header 默认显示在页面顶部
     * （见 concepts-layouts.md "When creating a new module, the module header will be
     * visible at the top of the screen"），因此 {@code visible} 取 true，
     * 而不是让前端拿到 null 后整块不渲染。
     *
     * @param moduleId 所属模块的 name，同时用作 {@link #id}
     */
    public static WorkshopHeader defaultOf(String moduleId) {
        WorkshopHeader header = new WorkshopHeader();
        header.id = moduleId;
        header.orientation = new HorizontalOrientation();
        header.collapseConfig = new NoneCollapseConfig();
        return header;
    }

    /**
     * 把两个多态字段的 null 补成默认实现，返回 this 便于链式调用。
     *
     * <p>为什么需要：{@code loadOrDefault} 只在<b>文件不存在</b>时才回落
     * {@link #defaultOf}。而多态字段是本类改造后才出现的，改造前落盘的 header XML 里
     * 没有 {@code orientation}/{@code collapseConfig} 元素，XStream 反序列化出来就是 null；
     * 字段上虽写了初始化表达式，但 XStream 不走构造器赋值。不补的话前端会拿到一个
     * 没有方向、没有折叠配置的 header，配置面板两个下拉都是空的。
     */
    public WorkshopHeader normalize() {
        if (this.orientation == null) {
            this.orientation = new HorizontalOrientation();
        }
        if (this.collapseConfig == null) {
            this.collapseConfig = new NoneCollapseConfig();
        }
        return this;
    }

    /**
     * 下发前端的 JSON 形态。
     *
     * <p>{@code DescribableJSON.getItemJson()} 产出 {@code {impl, displayName, vals}}：
     * {@code impl} 是<b>本类</b>的 FQCN，{@code vals} 是平铺字段，其中
     * {@code orientation} / {@code collapseConfig} 各自以自己的 {@code impl} 表达具体子类
     * （如 {@code VerticalOrientation} / {@code CollapsibleConfig}），前端据此知道该渲染
     * 哪套子字段。这与 {@code WorkshopVariable.toVariableJSON} 的下发形态一致。
     *
     * <p>改造前这里是手写字段清单的平铺 JSON，且 {@code orientation} 被转成小写枚举名 ——
     * 现在方向由 {@code impl} 承担，不再需要任何大小写转换；手写清单也一并去掉，
     * 免得以后加字段时漏写（漏了只是前端看不到，不会有任何报错）。
     */
    public static JSONObject toJSON(WorkshopHeader header) throws Exception {
        return new DescribableJSON<>(header).getItemJson();
    }

    @TISExtension
    public static class DefaultDescriptor extends Descriptor<WorkshopHeader> {
        @Override
        public String getDisplayName() {
            return "Workshop Header";
        }
    }
}
