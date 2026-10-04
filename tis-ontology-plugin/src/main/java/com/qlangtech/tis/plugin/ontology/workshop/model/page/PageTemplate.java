package com.qlangtech.tis.plugin.ontology.workshop.model.page;

import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopPage;

/**
 * 页面模板枚举，取值挂在 {@link WorkshopPage#template} 上。
 * <p>
 * 对应 Foundry Workshop 中页面底部那个 layout template picker
 * （"Try a layout template! Hover to preview layout"，见 workshop 设计文档 concepts-layouts 一节）。
 * 它回答的是<b>本页的直接子 Section 如何排布</b>（上下还是左右、分几栏），和 Foundry 一样：
 * 选模板 → 页面布局即变（"select that icon; the page layout will update to the one you selected"）。
 * 前端的选择入口是 {@code PageTemplatePickerComponent}
 * （tis-console 的 workshop/components/editor/page-template-picker/），
 * 一张悬浮在画布底部的卡片，单击即应用。
 * <p>
 * 语义落在<b>两个</b>地方，两边都要看，只改一处会得到一个"名字变了、样子没变"的模板：
 * <ol>
 *   <li><b>建页那一刻的骨架</b> —— tis-console 的 {@code ModuleStateService.seedSectionsByTemplate()}，
 *       决定 {@code applyAddPage} 往新页面里铺几个初始 Section。这是写侧。</li>
 *   <li><b>渲染期的页面排布</b> —— {@code workshop-page.component.less} 里的 {@code .page-template-*}
 *       系列，类名由 {@code workshop-page.component.ts#pageLayoutClass()} 拼出。这是读侧，
 *       也是模板的主要作用面：用户后续往页面里加/删 Section，栏数由模板固定不变。</li>
 * </ol>
 * 表单上的 label / help 声明在资源文件 {@code resources/.../workshop/model/WorkshopPage.json} 的
 * {@code template} 段。
 *
 * <h3>各枚举项语义</h3>
 * <p>
 * Foundry 选择器上只有五个（Details / Grid / Inbox / Overview / Settings，见
 * {@code detail/resources/configure_new_page.png}），本枚举除这五个的对应项外还留了
 * {@link #BLANK}（默认态）与 {@link #TWO_COLUMN} / {@link #THREE_COLUMN}（纯栅格）。
 * <ul>
 * 每一项的两段说明分工是：<b>业务用途</b>（来自 Foundry 的定位，决定用户什么时候选它）与
 * <b>当前实现口径</b>（TIS 实际铺出来的样子）。两者不必重合 —— 业务用途是选型依据，
 * 实现口径才是用户点下去会看到的东西，栅格的权威表述见下一节的映射表。
 * <ul>
 *   <li>{@link #BLANK}：空白页。对齐 Foundry <b>未配置</b>页的默认态 —— header 之下左右并排
 *       两个空 Section（截图 {@code configure_new_page.png}）。并排由本模板的栅格产生，
 *       不是靠嵌套一个容器 Section 实现的：两个 Section 是<b>同级的直接子节点</b>。
 *       注意它是"两块并排的空 Section"，不是"空画布"：字面读作"空白页"容易误解。
 *       <p>
 *       实现口径：左 30% / 右自适应（比例实测自截图，分隔线落在内容区 29.8% 处），
 *       建页铺两个名为「分区 1」「分区 2」的 rows Section。</li>
 *   <li>{@link #DETAILS}：详情布局。以<b>单个对象</b>为中心 —— 顶部对象属性，中部图表，
 *       底部关联对象列表与操作按钮。一般由 INBOX / GRID 经 Layout event 或 Overlay 跳进来。
 *       <p>
 *       实现口径：左固定 260px 导航栏 + 右自适应内容区。用固定 px 而非百分比，
 *       是因为导航栏本就不该随窗口缩放。</li>
 *   <li>{@link #GRID}：网格布局。只读监控看板 —— 指标卡 + 图表/表格铺成网格，基本无写入交互。
 *       对应 Foundry 的 Grid 与 Design Hub 的 Metrics Dashboard。
 *       <p>
 *       实现口径：列数不写死（{@code auto-fit minmax(280px,1fr)}，按可用宽度自动排），
 *       首个子项通栏当筛选条。</li>
 *   <li>{@link #INBOX}：收件箱布局。左列表 + 右详情/操作的分诊工作台，对应 Foundry 的 Inbox
 *       与 Design Hub 的 Alert Inbox。左侧当队列，右侧做处置，通常配 Overlay 钻取。
 *       <p>
 *       实现口径：左 340px 列表栏 + 右自适应。列表栏比 DETAILS 的导航栏宽一档 ——
 *       它要放得下标题与摘要。</li>
 *   <li>{@link #OVERVIEW}：概览布局。指标卡与图表铺成整体的态势页，对应 Foundry 的 Overview。
 *       <p>
 *       实现口径：<b>不是</b>自适应网格，而是固定等宽两栏 + 首个子项通栏。
 *       与 {@link #TWO_COLUMN} 的唯一区别就是那条通栏 —— 两者若都写成纯两栏会分不出来。</li>
 *   <li>{@link #SETTINGS}：设置布局。表单字段铺陈的配置页，对应 Foundry 的 Settings。
 *       <p>
 *       实现口径：首个子项通栏（标题）+ 下方左标签 / 右表单，比例 1:2（表单区更宽）。
 *       与 {@link #DETAILS} 的区别在于：DETAILS 没有通栏、左侧是固定宽导航，
 *       SETTINGS 有通栏、下方是按比例分的两栏。</li>
 *   <li>{@link #TWO_COLUMN}：两栏布局。无业务语义的纯栅格 —— 主内容 + 侧栏。</li>
 *   <li>{@link #THREE_COLUMN}：三栏布局。三段式，典型即 inbox 的细分形态：筛选栏 | 列表 | 详情。</li>
 * </ul>
 * {@link #TWO_COLUMN} / {@link #THREE_COLUMN} 在 Foundry 没有对应项，是 TIS 自己加的；它们只声明栏数、
 * 不绑定用途，日后若确认无用可以收掉。
 *
 * <h3>模板到页面排布的映射</h3>
 * <p>
 * 下表是<b>权威口径</b>，三个落地处都必须与它一致 —— 改一处就要改三处：
 * <ol>
 *   <li>渲染期栅格：{@code workshop-page.component.less} 的 {@code .page-template-*} 系列</li>
 *   <li>建页骨架：{@code ModuleStateService.seedSectionsByTemplate()}</li>
 *   <li>选择卡上的栏宽示意图：{@code page-template-picker.component.ts} 的 {@code .m-*} 系列
 *       （比例是同比缩写的示意值，但<b>相对宽窄必须一致</b>，否则卡片会画出与结果不符的样子）</li>
 * </ol>
 * <table border="1">
 *   <caption>页面模板语义</caption>
 *   <tr>
 *     <th>枚举</th><th>页面栅格（{@code grid-template-columns}）</th><th>建页初始 Section</th>
 *   </tr>
 *   <tr>
 *     <td>{@link #BLANK}</td>
 *     <td>{@code 30% minmax(0,1fr)}</td>
 *     <td>分区 1 ｜ 分区 2</td>
 *   </tr>
 *   <tr>
 *     <td>{@link #DETAILS}</td>
 *     <td>{@code 260px minmax(0,1fr)}</td>
 *     <td>导航栏 ｜ 主内容 →（概要、关联）</td>
 *   </tr>
 *   <tr>
 *     <td>{@link #GRID}</td>
 *     <td>{@code repeat(auto-fit, minmax(280px,1fr))}，首个子项通栏</td>
 *     <td>筛选条 + 区块 1–4</td>
 *   </tr>
 *   <tr>
 *     <td>{@link #INBOX}</td>
 *     <td>{@code 340px minmax(0,1fr)}</td>
 *     <td>收件列表 ｜ 详情 →（操作条、详情内容）</td>
 *   </tr>
 *   <tr>
 *     <td>{@link #OVERVIEW}</td>
 *     <td>{@code repeat(2, minmax(0,1fr))}，首个子项通栏</td>
 *     <td>顶部概览（通栏）｜ 主区 ｜ 侧栏</td>
 *   </tr>
 *   <tr>
 *     <td>{@link #SETTINGS}</td>
 *     <td>{@code minmax(0,1fr) minmax(0,2fr)}，首个子项通栏</td>
 *     <td>顶部标题（通栏）｜ 设置分组 ｜ 表单</td>
 *   </tr>
 *   <tr>
 *     <td>{@link #TWO_COLUMN}</td>
 *     <td>{@code repeat(2, minmax(0,1fr))}</td>
 *     <td>分区 1 ｜ 分区 2</td>
 *   </tr>
 *   <tr>
 *     <td>{@link #THREE_COLUMN}</td>
 *     <td>{@code repeat(3, minmax(0,1fr))}</td>
 *     <td>分区 1 ｜ 分区 2 ｜ 分区 3</td>
 *   </tr>
 * </table>
 * 窄屏（&le;768px）下所有多栏排布一律回落单栏，避免手机上挤成读不了的宽度。
 *
 * <h3>为什么"栅格 + 骨架"缺一不可</h3>
 * <p>
 * Foundry 选择器上那五个模板，读到图标里其实<b>只有三种栅格</b>：{@link #DETAILS} 与
 * {@link #INBOX} 同为"窄栏 + 宽内容"，{@link #OVERVIEW} 与 {@link #SETTINGS} 同为
 * "通栏 + 左右"，{@link #GRID} 是网格。也就是说，<b>光靠 CSS 无法让五个模板两两区分</b> ——
 * 把 DETAILS 和 INBOX 都映射成"左窄右宽"之后，用户在两者之间切换将看不出任何变化。
 * 它们真正的差别在<b>建好之后页面里有哪些 Section、怎么分层</b>，这正是上表第三列的由来。
 * <p>
 * 反过来，骨架也替代不了栅格：Section 自己的 {@code layout: 'columns'} 只能等宽
 * （{@code repeat(N, 1fr)}，N 取自 {@code layoutConfig.columnCount}），表达不了
 * "左窄 + 右宽"这种不等宽排布。所以宽度关系只能由页面栅格表达，骨架负责的是
 * "分成几块、谁在谁里面"。
 * <p>
 * 通栏（表格里"首个子项通栏"）目前是 CSS {@code :first-child} 实现的纯视觉规则，
 * 没有对应的模型字段。代价是<b>用户把第一个 Section 拖走之后通栏会跟着易主</b>——
 * 已知并接受。要根治需给 {@code WorkshopSection} 加一个显式的跨栏标记。
 *
 * <h3>注意事项</h3>
 * <ol>
 *   <li><b>别和 {@code SectionLayout} 搞混 —— 两者是不同层级的容器属性。</b>
 *       本枚举排的是<b>一个页面的直接子 Section</b>；{@code SectionLayout}
 *       （columns / rows / tabs / flow / toolbar / loop）排的是<b>某个 Section 自己的 childSections</b>。
 *       类比 CSS：本枚举之于页面如同 {@code grid-template-columns} 之于 grid 容器，两者的作用域
 *       是"容器 → 它的直接子元素"，各管各的、互不约束 —— 一个两栏页面里塞 3 个 Section，
 *       第三个自然换行；Section 里嵌 Section 用什么排布，也与本枚举无关。</li>
 *   <li><b>八个模板都有建页骨架，但骨架只在建页那一刻生成一次。</b>
 *       之所以只能在建页时生成，是因为建页之后 {@code WorkshopPage#sections} 由别的存储独立保管、
 *       页面只存 template 这一个标记，回读时反推不出骨架。
 *       因此<b>切换模板只换栅格、不重铺 Section</b>（见 tis-console 的
 *       {@code WorkshopPageComponent.onTemplatePick}）—— 重铺会把用户已经放好的 widget 全部抹掉，
 *       而模板是随时可切的开关，代价完全不成比例。
 *       <p>
 *       另需注意：{@code WorkshopModule.pageToJSON} 对 null 的 template 兜了 {@code "blank"}，
 *       但那只是下发时的兜底，内存里的对象仍可能是 null；前端 {@code pageLayoutClass()}
 *       也照此再兜一次。</li>
 *   <li><b>后端侧本字段只做存取与回显</b>（{@code WorkshopModule} 中把 {@code name().toLowerCase()}
 *       塞进 JSON），不参与任何分支 —— 排布与骨架完全在前端落地。也正因如此，
 *       {@code WorkshopEditorService} 的 kind 白名单里没有页面级操作，
 *       「切换模板」这条操作目前<b>存不下去</b>（详见该类注释）。</li>
 *   <li><b>取值大小写不统一（遗留问题）。</b>后端下发走的是 {@code name().toLowerCase()}，
 *       因此 {@link #TWO_COLUMN} / {@link #THREE_COLUMN} 的值是 {@code two_column} / {@code three_column}，
 *       而其余六个是单词、天然与前端字面量一致。设计说明书里原枚举本带一个显式的 value 字段
 *       （{@code TWO_COLUMN("twoColumn", "两栏布局")}），实现时省掉了。前端 {@code PageTemplate} union
 *       已按后端实际下发值对齐成下划线形式；若要改回 camelCase，需给本枚举补回 value 字段并改用
 *       它的两处序列化点（{@code WorkshopModule} 的 pageToJSON 与 module 内嵌 pages 的拼装）。</li>
 * </ol>
 *
 * @see WorkshopPage
 */
public enum PageTemplate implements DescriptorUseableShortComment {
    BLANK("空白页"),
    DETAILS("详情布局"),
    GRID("网格布局"),
    INBOX("收件箱布局"),
    OVERVIEW("概览布局"),
    SETTINGS("设置布局"),
    TWO_COLUMN("两栏布局"),
    THREE_COLUMN("三栏布局");

    private final String comment;

    PageTemplate(String comment) {
        this.comment = comment;
    }

    @Override
    public String shortComment() {
        return comment;
    }
}