package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.model.page.PageTemplate;
import com.qlangtech.tis.plugin.ontology.workshop.store.WorkshopPageStore;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Workshop Page 实体
 * <p>
 * sections 使用 transient 修饰，不随 WorkshopPage 主 JSON 序列化。
 * <p>
 * 落盘走 {@link WorkshopPageStore}（一个页面一个文件：
 * {@code ontology/{domain}/workshop/{moduleName}/pages/{id}.xml}）。读写由模块加载时回填
 * （{@code WorkshopModule.load}）和编辑器保存（{@code module-apply} 增量 /
 * {@code module-save} 兜底快照）驱动，不再有逐字段 patch 写入口。
 */
public class WorkshopPage implements Describable<WorkshopPage>, Serializable, IdentityName {

    private static final long serialVersionUID = 1L;

    /**
     * identity 字段：页面 UUID，由前端建页时生成（{@code ModuleStateService.applyAddPage}
     * 用 {@code crypto.randomUUID()}），后端把它当不透明值原样持久化。
     * <p>
     * <b>不提供默认值</b>：id 的生成方是实体的创建者（前端），后端只负责透传与持久化；
     * {@link #identityValue()} 在 id 为空时直接抛异常，不兜底生成 —— 兜底会掩盖
     * 「id 在链路上丢失」这类缺陷，并让同一条数据在不同请求里拿到不同的身份。
     * <p>
     * 表单里以 {@code readonly} 展示，不可编辑。
     * <p>
     * <b>刻意不加 {@link Validator#identity}</b>：那条规则除了字符集，还带一条
     * 「不能以数字开头」的限制，而 {@code crypto.randomUUID()} 有 10/16 的概率以数字开头 ——
     * 加上它会让「保存页面」随机失败。id 是前端生成的不透明值，不是用户输入的名称，
     * 格式约束（会拒绝合法 UUID）比它挡下的错误更多。{@link Validator#require} 足够。
     */
    @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT,
            validate = {Validator.require})
    public String id;

    /**
     * 页面显示名称，<b>可编辑</b>。
     * <p>
     * 模块内一切对 page 的引用（{@code SwitchToPageConfig.pageId}、
     * {@code WidgetTabConfig.targetPage}、{@code PageRoutingConfig.targetPage}、
     * {@code page-get} 的寻址参数、{@code module-apply} / {@code module-save} 的写入键）用的都是 {@link #id}，
     * name 不参与任何寻址 —— 改名不会让已有引用悬空。
     */
    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String name;

    @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT)
    public String displayName;

    @FormField(ordinal = 3, type = FormFieldType.ENUM, validate = {Validator.require})
    public PageTemplate template = PageTemplate.BLANK;

    @FormField(ordinal = 4, type = FormFieldType.INT_NUMBER)
    public Integer sortOrder = 0;

    // @FormField(ordinal = 10, type = FormFieldType.MULTI_SELECTABLE)
    public transient List<WorkshopSection> sections = new ArrayList<>();

    /**
     * @param pageId 页面主键，即 {@link #id}
     * @return 不存在时返回 {@code null}
     */
    public static WorkshopPage loadPage(String ontologyDomainId, String moduleName, String pageId) {
        return WorkshopPageStore.create(ontologyDomainId, moduleName).load(pageId);
    }

    /**
     * 加载某个模块下的全部页面，按 {@code sortOrder} 升序。
     */
    public static List<WorkshopPage> loadAll(String ontologyDomainId, String moduleName) {
        return WorkshopPageStore.create(ontologyDomainId, moduleName).listAll();
    }

    /**
     * @throws IllegalStateException id 为空 —— 说明创建方（前端建页）没有下发 id，
     *                               属于链路上的缺陷，此处不兜底生成
     */
    @Override
    public String identityValue() {
        if (this.id == null) {
            throw new IllegalStateException("WorkshopPage id can not be null");
        }
        return this.id;
    }

    /**
     * 把 section 挂到本页下，并同步维护子侧外键
     * {@link AbstractWorkshopSection#parent}（PAGE + page id）—— 内存集合与落盘外键
     * 只在这一处同时写入。
     *
     * @throws IllegalStateException 本 page 还没有 id（见 {@link #identityValue()}）
     */
    public void addSection(WorkshopSection section) {
        if (sections == null) {
            sections = new ArrayList<>();
        }
        section.parent = WorkshopCptParentId.pageParent(identityValue());
        sections.add(section);
    }

    /**
     * 本页顶层 section 的序号归一化 —— {@code sortOrder} 是「父容器内序号」，
     * 重排后压实成 0..n-1。加载重建或拖拽排序之后调用。
     */
    public void normalizeSectionOrder() {
        AbstractWorkshopSection.normalizeOrder(sections,
                s -> s.sortOrder == null ? 0 : s.sortOrder,
                (s, index) -> s.sortOrder = index);
    }

    @TISExtension
    public static class DefaultDescriptor extends Descriptor<WorkshopPage> {
        @Override
        public String getDisplayName() {
            return "Workshop Page";
        }
    }
}

