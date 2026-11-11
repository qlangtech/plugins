package com.qlangtech.tis.plugin.ontology.workshop.model.page;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopPage;

import java.io.Serializable;

/**
 * 页面模板抽象基类，定义页面的直接子 Section 排布规则。
 *
 * <p>对应 Foundry Workshop 中页面底部的 layout template picker。
 * 它回答的是<b>本页的直接子 Section 如何排布</b>（上下还是左右、分几栏、各栏宽度），
 * 取值挂在 {@link WorkshopPage#template} 上。
 *
 * <h3>从枚举到插件类的演进</h3>
 * <p>原枚举 {@code PageTemplate} 包含 8 个固定布局（BLANK / DETAILS / INBOX / TWO_COLUMN 等），
 * 经实践发现其中 BLANK / DETAILS / INBOX / TWO_COLUMN 本质都是"左右两栏 + 比例不同"，
 * 而 OVERVIEW / SETTINGS 也只是"两栏 + 首个通栏"的变体。
 * 固定枚举无法表达"左侧占 25%"这类用户需求，改为插件类后：
 * <ul>
 *   <li>原枚举项收敛成 4 个派生类（{@link TwoColumnPageTemplate} / {@link HeaderContentPageTemplate} /
 *       {@link GridPageTemplate} / {@link ThreeColumnPageTemplate}）
 *   <li>派生类可以声明配置字段（如左栏宽度、上边栏高度），用户建页时填写
 *   <li>前端按派生类的 impl 名称与配置字段动态拼装 CSS grid
 * </ul>
 *
 * <h3>派生类职责</h3>
 * <p>每个派生类提供三样东西，分别落在三处、缺一不可：
 * <ol>
 *   <li><b>配置字段</b>（可选）—— 如 {@link TwoColumnPageTemplate#leftColumnWidth}、
 *       {@link GridPageTemplate#minColumnWidth}，由用户在页面配置面板里填写；
 *   <li><b>建页骨架</b> —— 前端 {@code ModuleStateService.PAGE_TEMPLATE_SEEDS} 按 impl 声明
 *       本模板要铺几个初始 Section（含每个分区的名称与 layout）。这份骨架<b>只在后端留档于
 *       各派生类的类 javadoc</b>，后端没有对应的查询入口 —— 骨架是「名称 + layout」的结构，
 *       一个整型的「分区数量」表达不了；
 *   <li><b>CSS 栅格</b> —— 两处：静态骨架在 {@code workshop-page.component.less} 的
 *       {@code .page-template-*} 系列，用户配置的栏宽 / 栏高由前端
 *       {@code page.model.ts#extractPageLayoutStyle} 算成内联样式覆盖上去。
 * </ol>
 * 后端<b>不</b>产出 CSS 字符串 —— 栅格表达式只在前端拼，避免两端各维护一份单位换算。
 *
 * <h3>序列化格式</h3>
 * <p>标准 TIS 插件形状（{@code {impl, vals}}），{@code impl} 由
 * {@link Descriptor#getId()} 产生，**恒为具体插件类的全限定名**（如
 * {@code com.qlangtech.tis.plugin.ontology.workshop.model.page.TwoColumnPageTemplate}），
 * 不是 kebab-case 短名：
 * <pre>{@code
 * {
 *   "impl": "com.qlangtech.tis.plugin.ontology.workshop.model.page.TwoColumnPageTemplate",
 *   "vals": {
 *     "leftColumnWidth": {
 *       "impl": "com.qlangtech.tis.plugin.ontology.workshop.model.page.PercentColumnWidth",
 *       "vals": {"percentage": 30}
 *     }
 *   }
 * }
 * }</pre>
 * 前端读路径见 {@code page.model.ts#extractPageLayoutStyle}（经 {@code WorkshopPageComponent#pageLayoutStyle}）。
 *
 * <h3>缺省模板</h3>
 * <p>页面的 {@code template} 可能缺失（手工编辑过的文件、或建页链路漏传），
 * 由 {@link WorkshopPage#readResolve()} 调 {@link #createDefault()} 兜成两栏 30%。
 * 缺省值只此一处定义，不要在别处再拼一份。
 *
 * @see WorkshopPage#template
 * @see TwoColumnPageTemplate
 * @see HeaderContentPageTemplate
 * @see GridPageTemplate
 */
public abstract class PageTemplate implements Describable<PageTemplate>, Serializable {

    private static final long serialVersionUID = 1L;

    /**
     * 缺省模板：两栏布局，左栏 30%。
     *
     * <p>供 {@link WorkshopPage#readResolve()} 在页面没有 template 时兜底 ——
     * XStream 不走构造器，字段初值对缺失节点无效，兜底只能放在反序列化钩子里。
     *
     * @return 新建的两栏模板实例（每次调用都是新对象，调用方可以自由修改）
     */
    public static PageTemplate createDefault() {
        TwoColumnPageTemplate template = new TwoColumnPageTemplate();
        PercentColumnWidth width = new PercentColumnWidth();
        width.percentage = 30;
        template.leftColumnWidth = width;
        return template;
    }

    public abstract static class BaseDescriptor extends Descriptor<PageTemplate> implements DescriptorUseableShortComment, IEndTypeGetter {
        // 子类 Descriptor 可以在这里添加通用方法
    }
}
