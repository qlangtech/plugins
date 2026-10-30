package com.qlangtech.tis.plugin.ontology.workshop;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.datax.IManipulateStatus;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.IStoreSelfManagement;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.lang.PayloadLink;
import com.qlangtech.tis.manage.common.IAjaxResult;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.IdentityDesc;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.manipulate.ManipulatePluginCacheRegister;
import com.qlangtech.tis.plugin.ontology.OntologyDomain;
import com.qlangtech.tis.plugin.ontology.OntologyDomainManipulate;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.plugin.ontology.workshop.enums.SectionParentType;
import com.qlangtech.tis.plugin.ontology.workshop.model.AbstractWorkshopSection;
import com.qlangtech.tis.plugin.ontology.workshop.model.AutoRefreshConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.ConditionalVisibilityOff;
import com.qlangtech.tis.plugin.ontology.workshop.model.RoutingConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.SectionChild;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopCptParentId;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopHeader;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopOverlay;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopPage;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopSection;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopVariable;
import com.qlangtech.tis.plugin.ontology.workshop.service.WorkshopModuleService;
import com.qlangtech.tis.plugin.ontology.workshop.store.AbstractWorkshopEntityStore;
import com.qlangtech.tis.plugin.ontology.workshop.store.WorkshopOverlayStore;
import com.qlangtech.tis.plugin.ontology.workshop.store.WorkshopPageStore;
import com.qlangtech.tis.plugin.ontology.workshop.store.WorkshopSectionStore;
import com.qlangtech.tis.plugin.ontology.workshop.store.WorkshopWidgetStore;
import com.qlangtech.tis.plugin.ontology.workshop.widget.WorkshopWidget;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.util.DescribableJSON;
import com.qlangtech.tis.util.IPluginContext;
import com.qlangtech.tis.util.RobustReflectionConverter2;
import com.qlangtech.tis.util.UploadPluginMeta;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Collectors;

import static com.qlangtech.tis.plugin.ontology.OntologyDomain.getOntologyDomainDir;

/**
 * Workshop Module 聚合根实体
 * <p>
 * 作为 TIS 插件实现，支持通过 UI 表单配置
 *
 * @see WorkshopPage
 * @see WorkshopSection
 * @see WorkshopWidget
 */
public final class WorkshopModule extends OntologyDomainManipulate implements IManipulateStatus //
        , IdentityDesc<JSONObject>, IPluginStore.BeforePluginSaved, IPluginStore.AfterPluginSaved {

    private static final Logger logger = LoggerFactory.getLogger(WorkshopModule.class);

    /**
     * 每个 (domain, module) 的数据版本号。写成功后由 {@link #invalidateWorkshopData} 递增。
     * <p>
     * 只存计数器、不持有模块实例 —— 失效动作绝不能反过来触发 domain 缓存加载。
     */
    private static final ConcurrentMap<String, Long> WORKSHOP_DATA_GENERATION = new ConcurrentHashMap<>();

    // 标识属性
    @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
    public String name;

    private String ontologyDomain;
    @FormField(ordinal = 1, type = FormFieldType.TEXTAREA)
    public String description;

//    @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
//    public String ontologyDomainId;

    // 模块级配置
    @FormField(ordinal = 3, type = FormFieldType.ENUM)
    public Boolean stateSavingEnabled = false;

    @FormField(ordinal = 4)
    public AutoRefreshConfig autoRefreshConfig;

    @FormField(ordinal = 5)
    public RoutingConfig routingConfig;
    // 聚合子实体（使用强类型 List）
    //
    // 这三个集合与 header 都是 transient、且 @FormField 被注释掉：子实体不走模块表单，
    // 而是各自独立落盘（pages → WorkshopPageStore、variables → WorkshopVariableStore、
    // header → WorkshopHeaderStore），读取时由 load()/loadAll() 回填。
    // WorkshopModule.json 里因此也没有这几个 key —— 它们不在表单上出现。
    // @FormField(ordinal = 10, type = FormFieldType.MULTI_SELECTABLE)
    public transient List<WorkshopPage> pages = new ArrayList<>();

    // @FormField(ordinal = 11, type = FormFieldType.MULTI_SELECTABLE)
    public transient List<WorkshopOverlay> overlays = new ArrayList<>();

    // @FormField(ordinal = 12, type = FormFieldType.MULTI_SELECTABLE)
    public transient List<WorkshopVariable> variables = new ArrayList<>();

    /**
     * 模块级页头配置（标题 / 方向 / 折叠等）。
     * <p>
     * 与 module 是 1:1，由 { WorkshopHeaderStore} 独立落盘
     * （{@code ontology/{domain}/workshop_headers/{moduleName}.xml}），
     * 不随模块 XML 走，理由见 { WorkshopHeaderStore} 类注释。
     * 前端配置入口在 Workshop 编辑器的 Layout 面板（对标 Palantir 的 Header 面板）。
     */
    public transient WorkshopHeader header;

    // 子实体回填状态（不落盘）。XStream 通过 readResolve() 触发一次回填；
    // 写侧递增 generation 后，下一次 load()/loadAll() 会重建。
    private transient boolean workshopDataLoaded;
    private transient long workshopDataGeneration = Long.MIN_VALUE;
    private transient String workshopDataKey;

    // 非表单字段（不需要 @FormField）
    private String id;
    private String createdBy;
    private LocalDateTime createdAt;
    private String updatedBy;
    private LocalDateTime updatedAt;

    public static File getWorkshopDir(String ontologyName, String workshop) {
        if (StringUtils.isEmpty(ontologyName)) {
            throw new IllegalArgumentException("param ontologyName can not be empty");
        }
        if (StringUtils.isEmpty(workshop)) {
            throw new IllegalArgumentException("param workshop can not be empty");
        }
        return new File(getOntologyDomainDir(ontologyName), OntologyDomain.KEY_WORKSHOP + File.separator + workshop);
    }

    /**
     * 本模块的全部页面，按 {@code sortOrder} 升序。数据由 {@link #loadPages} 在
     * {@link #load} / {@link #loadAll} 时从独立存储回填。
     */
    public List<WorkshopPage> loadAllPage() {
        List<WorkshopPage> result = pages == null ? new ArrayList<>() : new ArrayList<>(pages);
        result.sort((a, b) -> {
            int ao = a.sortOrder != null ? a.sortOrder : 0;
            int bo = b.sortOrder != null ? b.sortOrder : 0;
            return Integer.compare(ao, bo);
        });
        return result;
    }


    public static WorkshopModule load(String ontologyDomain, String modeuleName) {
        ManipulatePluginCacheRegister.TemplateManipulateStore<OntologyDomainManipulate> manipulateStore
                = getManipulateStore(ontologyDomain, false);
        WorkshopModule module = manipulateStore.getManipuldate(IdentityName.create(modeuleName), WorkshopModule.class);
        if (module != null) {
            module.ensureWorkshopData(ontologyDomain, modeuleName);
        }
        return module;
    }

    public static List<WorkshopModule> loadAll(String ontologyDomain) {
        ManipulatePluginCacheRegister.TemplateManipulateStore<OntologyDomainManipulate> manipulateStore
                = getManipulateStore(ontologyDomain, false);
        List<WorkshopModule> modules = manipulateStore.getManipuldaties(WorkshopModule.class);
        for (WorkshopModule module : modules) {
            if (module != null) {
                module.ensureWorkshopData(ontologyDomain, module.name);
            }
        }
        return modules;
        //  return manipulateStore.getManipuldate(IdentityName.create(modeuleName), WorkshopModule.class);
    }

    private static String workshopDataKey(String ontologyDomain, String moduleName) {
        return ontologyDomain + ' ' + moduleName;
    }

    /**
     * 页面/分区/浮层/Widget 写成功后调用，递增该模块的数据版本号。
     * <p>
     * 只改计数器，不做任何缓存查找或加载 —— 可以从任意写路径安全调用。
     */
    public static void invalidateWorkshopData(String ontologyDomain, String moduleName) {
        if (StringUtils.isEmpty(ontologyDomain) || StringUtils.isEmpty(moduleName)) {
            return;
        }
        WORKSHOP_DATA_GENERATION.merge(workshopDataKey(ontologyDomain, moduleName), 1L, Long::sum);
    }

    /**
     * 回填子实体：每个模块实例只做一次；写入后 generation 变化则重建。
     * <p>
     * 用 {@code synchronized} 串行化同一实例的重建 —— XStream 反序列化与业务读取
     * 可能并发进入，{@link #loadLayout} 的整体赋值也依赖这一点。
     */
    synchronized void ensureWorkshopData(String ontologyDomain, String moduleName) {
        if (StringUtils.isEmpty(ontologyDomain) || StringUtils.isEmpty(moduleName)) {
            return;
        }
        final String key = workshopDataKey(ontologyDomain, moduleName);
        // 必须在加载前捕获版本号：加载期间若有并发写递增，宁可下次多读一次，
        // 也不能把旧数据标记成最新
        final long observedGeneration = WORKSHOP_DATA_GENERATION.getOrDefault(key, 0L);
        if (workshopDataLoaded && key.equals(workshopDataKey)
                && workshopDataGeneration == observedGeneration) {
            return;
        }
        loadVariables(ontologyDomain, moduleName, this);
        loadHeader(ontologyDomain, moduleName, this);
        loadPages(ontologyDomain, moduleName, this);
        loadLayout(ontologyDomain, moduleName, this);
        this.workshopDataKey = key;
        this.workshopDataGeneration = observedGeneration;
        this.workshopDataLoaded = true;
    }

    /**
     * XStream 反序列化完成后的回调（机制见 {@code PluginStore#load()} 中 unmarshal 处注释）。
     * <p>
     * 子实体（pages/sections/variables/header）是 transient，模块 XML 里没有；
     * 在这里一次性回填，避免每次业务读取都重新扫盘并重复挂载。
     */
    private Object readResolve() {
        // 插件元数据扫描 / dry-run 场景不做 I/O
        if (!RobustReflectionConverter2.usedPluginInfo.get().isCacheable()) {
            return this;
        }
        final String domain = StringUtils.trimToNull(this.ontologyDomain);
        final String module = StringUtils.trimToNull(this.name);
        // 老 XML 没有 ontologyDomain 或运行时尚未初始化：跳过，
        // 由 WorkshopModule.load(domain, name) 携带权威参数兜底
        if (domain == null || module == null || TIS.get() == null) {
            return this;
        }
        try {
            ensureWorkshopData(domain, module);
        } catch (RuntimeException | LinkageError e) {
            // 单个损坏的子文件不能拖垮整个 domain store 的加载；
            // load() 仍是权威路径，会在请求线程里重试并暴露真实错误
            logger.warn("preload workshop data failed, domain:{}, module:{}; will retry on load()",
                    domain, module, e);
        }
        return this;
    }

    /**
     * 从独立存储回填 {@link #pages}。
     * <p>
     * pages 是 transient 字段（模块 XML 里没有），不回填的话 doGet 下发不了页面列表，
     * {@code WidgetOptionHelper#getWorkshopPageOptions} 的页面下拉也恒为空。
     * <p>
     * 每个模块实例只回填一次（反序列化时由 {@link #readResolve()} 触发）；
     * 页面写成功后 {@link #invalidateWorkshopData} 递增版本号，下一次读取会重建。
     */
    private static void loadPages(String ontologyDomain, String moduleName, WorkshopModule module) {
        if (module == null || StringUtils.isEmpty(ontologyDomain) || StringUtils.isEmpty(moduleName)) {
            return;
        }
        module.pages = WorkshopPage.loadAll(ontologyDomain, moduleName);
    }

    /**
     * 从独立存储回填 {@link #overlays}，并把 section / widget 挂回各自的父容器，重建编辑器需要的整棵树。
     *
     * <p>三类实体各自独立落盘（{@link WorkshopOverlayStore} / {@link WorkshopSectionStore} /
     * {@link WorkshopWidgetStore}），归属关系由子侧外键表达：
     * <ul>
     *   <li>section → {@link AbstractWorkshopSection#parent}（PAGE / OVERLAY）</li>
     *   <li>widget → {@link WorkshopWidget#sectionId}</li>
     * </ul>
     * 必须在 {@link #loadPages} 之后调用 —— 挂 section 需要页面对象已经就位。
     *
     * <p>子分区（{@link SectionChild}）不是独立实体，它们嵌在父 section 的 {@code childSections} 里
     * 随同一个 XML 落盘，因此不参与挂载，只登记到「id → section」的索引里，供 widget 挂载时查找。
     *
     * <p><b>重建语义</b>：page/section/widget 实例被 {@code PluginStore} 缓存复用，不能在它们上面
     * {@code add} / {@code clear} —— 那样每次读取都会重复追加（历史 OOM 的根因）。这里先在本地
     * 构建完整列表并归一化，最后整体替换引用；并发重建最多是两份完整快照的竞争，长度始终受
     * 落盘实体数约束。
     */
    private static void loadLayout(String ontologyDomain, String moduleName, WorkshopModule module) {
        if (module == null || StringUtils.isEmpty(ontologyDomain) || StringUtils.isEmpty(moduleName)) {
            return;
        }

        Map<String, WorkshopPage> pages = new LinkedHashMap<>();
        if (module.pages != null) {
            for (WorkshopPage page : module.pages) {
                pages.put(page.identityValue(), page);
            }
        }

        module.overlays = new ArrayList<>(WorkshopOverlayStore.create(ontologyDomain, moduleName).listAll());
        Map<String, WorkshopOverlay> overlays = new LinkedHashMap<>();
        for (WorkshopOverlay overlay : module.overlays) {
            overlays.put(overlay.identityValue(), overlay);
        }

        // 父容器 → 子 section 的本地桶。预置所有页面/浮层：没有子项的父容器也会拿到空 list，
        // 从而清掉上一次挂载残留的引用。
        Map<String, List<WorkshopSection>> sectionsByPageId = new LinkedHashMap<>();
        for (String pageId : pages.keySet()) {
            sectionsByPageId.put(pageId, new ArrayList<>());
        }
        Map<String, List<WorkshopSection>> sectionsByOverlayId = new LinkedHashMap<>();
        for (String overlayId : overlays.keySet()) {
            sectionsByOverlayId.put(overlayId, new ArrayList<>());
        }

        List<WorkshopSection> sections = WorkshopSectionStore.create(ontologyDomain, moduleName).listAll();
        Map<String, AbstractWorkshopSection> sectionsById = new LinkedHashMap<>();
        for (WorkshopSection section : sections) {
            WorkshopCptParentId parent = section.parent;
            if (parent == null) {
                // 归属字段缺失的半残数据：挂不到任何父容器上（文件仍在，下一次快照保存按差集处理）
                continue;
            }
            switch (parent.parentType) {
                case PAGE -> {
                    List<WorkshopSection> bucket = sectionsByPageId.get(parent.parentId);
                    if (bucket != null) {
                        bucket.add(section);
                    }
                }
                case OVERLAY -> {
                    List<WorkshopSection> bucket = sectionsByOverlayId.get(parent.parentId);
                    if (bucket != null) {
                        bucket.add(section);
                    }
                }
                case SECTION -> {
                    // 子分区随父 section 落盘，独立文件中的记录不应出现在顶层树里
                    continue;
                }
            }
            sectionsById.put(section.identityValue(), section);
            if (section.childSections != null) {
                for (SectionChild child : section.childSections) {
                    sectionsById.put(child.identityValue(), child);
                }
            }
        }

        // widget 同样先按 sectionId 归到本地桶，再整体赋给 section.widgets
        Map<String, List<WorkshopWidget>> widgetsBySectionId = new LinkedHashMap<>();
        for (WorkshopWidget widget : WorkshopWidgetStore.create(ontologyDomain, moduleName).listAll()) {
            if (widget.sectionId == null || !sectionsById.containsKey(widget.sectionId)) {
                continue;
            }
            widgetsBySectionId.computeIfAbsent(widget.sectionId, k -> new ArrayList<>()).add(widget);
        }

        // 子侧外键 + 序号归一化（只改标量字段，不动集合）
        for (Map.Entry<String, List<WorkshopSection>> entry : sectionsByPageId.entrySet()) {
            normalizeSections(entry.getValue(), WorkshopCptParentId.pageParent(entry.getKey()));
        }
        for (Map.Entry<String, List<WorkshopSection>> entry : sectionsByOverlayId.entrySet()) {
            normalizeSections(entry.getValue(), WorkshopCptParentId.overlayParent(entry.getKey()));
        }

        // 所有 section（含子分区、含孤儿）都整体替换 widgets —— 长度永远不超过落盘实体数
        for (WorkshopSection section : sections) {
            section.widgets = takeWidgets(widgetsBySectionId, section.identityValue());
            section.normalizeChildSectionOrder();
            normalizeConditionalVisibility(section);
            if (section.childSections != null) {
                for (SectionChild child : section.childSections) {
                    child.widgets = takeWidgets(widgetsBySectionId, child.identityValue());
                    normalizeConditionalVisibility(child);
                }
            }
        }

        // 整体赋值（不 clear、不 append）：并发重建只会留下某一份完整快照
        for (WorkshopPage page : pages.values()) {
            page.sections = sectionsByPageId.getOrDefault(page.identityValue(), new ArrayList<>());
            page.normalizeSectionOrder();
        }
        for (WorkshopOverlay overlay : overlays.values()) {
            overlay.sections = sectionsByOverlayId.getOrDefault(overlay.identityValue(), new ArrayList<>());
            overlay.normalizeSectionOrder();
        }
    }

    private static void normalizeSections(List<WorkshopSection> sections, WorkshopCptParentId parent) {
        for (WorkshopSection section : sections) {
            section.parent = parent;
        }
        AbstractWorkshopSection.normalizeOrder(sections,
                s -> s.sortOrder == null ? 0 : s.sortOrder,
                (s, index) -> s.sortOrder = index);
    }

    /**
     * 条件可见性缺失时补 off —— XStream 反序列化不走字段初始化，旧 XML 里没有该元素时为
     * {@code null}；服务端对象补成 off 后满足「默认不启用」的不变量（前端同样把 null 视为 off）。
     */
    private static void normalizeConditionalVisibility(AbstractWorkshopSection section) {
        if (section.conditionalVisibility == null) {
            section.conditionalVisibility = new ConditionalVisibilityOff();
        }
    }

    private static List<WorkshopWidget> takeWidgets(
            Map<String, List<WorkshopWidget>> widgetsBySectionId, String sectionId) {
        List<WorkshopWidget> widgets = new ArrayList<>(
                widgetsBySectionId.getOrDefault(sectionId, Collections.emptyList()));
        AbstractWorkshopSection.normalizeOrder(widgets,
                w -> w.sortOrder == null ? 0 : w.sortOrder,
                (w, index) -> w.sortOrder = index);
        return widgets;
    }

    /**
     * 从独立存储回填 {@link #variables}。
     * <p>
     * variables 是 transient 字段，模块 XML 里没有它（XStream 会跳过 transient），
     * 因此必须在这里补上，否则 {@link #findVariableByName(String)} 恒为 null，
     * WidgetOptionHelper 的变量下拉也拿不到数据。
     */
    private static void loadVariables(String ontologyDomain, String moduleName, WorkshopModule module) {
        if (module == null || StringUtils.isEmpty(ontologyDomain) || StringUtils.isEmpty(moduleName)) {
            return;
        }
        //  module.variables = WorkshopVariableStore.create(ontologyDomain, moduleName).listAll();

        module.variables = WorkshopVariable.loadAll(ontologyDomain, moduleName);
    }

    /**
     * 从独立存储回填 {@link #header}。
     * <p>
     * header 与 variables 同理是 transient 字段，模块 XML 里没有它，不回填的话
     * doGet 下发的 JSON 里就没有 header，前端 {@code module-container.component.html}
     * 的 {@code *ngIf="module.header?.vals?.visible"} 恒为 false —— 整个
     * {@code <app-workshop-header>} 不渲染。
     * <p>
     * 尚未配置过时回落到默认 header（{@code visible = true}）而不是 null：
     * 前端要靠它渲染出顶部工具栏，编辑器也要靠它才有可编辑的对象。
     */
    private static void loadHeader(String ontologyDomain, String moduleName, WorkshopModule module) {
        if (module == null || StringUtils.isEmpty(moduleName)) {
            return;
        }
        module.header = WorkshopHeader.loadOrDefault(ontologyDomain, moduleName);
    }

    @Override
    public void beforeSaved(IPluginContext pluginContext, Optional<Context> context) {
        OntologyPluginMeta pluginMeta = OntologyPluginMeta.createPluginMeta();
        this.ontologyDomain = Objects.requireNonNull(pluginMeta, "pluginMeta can not be null").getDomain();
        if (StringUtils.isEmpty(this.id)) {
            this.id = UUID.randomUUID().toString();
        }
        if (this.createdAt == null) {
            this.createdAt = LocalDateTime.now();
        }
        this.updatedAt = LocalDateTime.now();
    }

    @Override
    public void afterSaved(IPluginContext pluginContext, Optional<Context> context) {

    }

    @Override
    public ManipulateStateSummary manipulateStatusSummary() {
        final StringBuilder summary = new StringBuilder("已经开启Workshop功能");
        return new ManipulateStateSummary(
                Collections.singletonList(IManipulateStatus.create("正常"))
                , summary.toString(), true);
    }

    /**
     * 'ontology/:domainName/workshop/:moduleId'
     *
     * @return
     */
    @Override
    public Optional<PayloadLink> manipulateManagerPath() {
        return Optional.of(new PayloadLink("查看状态", "/ontology/" + this.ontologyDomain + "/workshop/" + this.name));
    }

    @Override
    public JSONObject describePlugin() {
        return Descriptor.getManipulateMeta(false, this);
    }

    @Override
    public void initialize() {

    }

    /**
     * @throws IllegalStateException name 为空 —— identity 字段没有值属于链路上的缺陷，
     *                               此处不兜底生成（取值恒为 {@link #name}，不做替换）
     */
    @Override
    public String identityValue() {
        if (this.name == null) {
            throw new IllegalStateException("WorkshopModule name can not be null");
        }
        return this.name;
    }

    // 业务方法
    public void addPage(WorkshopPage page) {
        if (pages == null) {
            pages = new ArrayList<>();
        }
        pages.add(page);
        this.updatedAt = LocalDateTime.now();
    }

    public void addVariable(WorkshopVariable variable) {
        if (variables == null) {
            variables = new ArrayList<>();
        }
        variables.add(variable);
        this.updatedAt = LocalDateTime.now();
    }

    public void addOverlay(WorkshopOverlay overlay) {
        if (overlays == null) {
            overlays = new ArrayList<>();
        }
        overlays.add(overlay);
        this.updatedAt = LocalDateTime.now();
    }

    /**
     * page-get 的寻址参数名 —— 装的是 {@link WorkshopPage#id}（UUID）。
     * <p>
     * 页面一律按 id 寻址：name 是可编辑的显示名，不能作为查表键。
     */
    public static final String PARAM_PAGE_ID = "pageId";

    /**
     * 快照里的页面数组键名
     */
    public static final String KEY_PAGES = "pages";
    /**
     * 快照里的浮层数组键名
     */
    public static final String KEY_OVERLAYS = "overlays";
    /**
     * 快照里的顶层分区数组键名（子分区嵌在父 section 的 {@code childSections} 里）
     */
    public static final String KEY_SECTIONS = "sections";
    /**
     * 快照里的 widget 数组键名
     */
    public static final String KEY_WIDGETS = "widgets";
    /**
     * section 的父容器信封字段（不是 {@code @FormField}，故与 {@code vals} 平级）
     */
    public static final String KEY_PARENT_ID = "parentId";
    public static final String KEY_PARENT_TYPE = "parentType";
    /**
     * widget 的所属 section 信封字段（同上，不是 {@code @FormField}）
     */
    public static final String KEY_SECTION_ID = "sectionId";

    /**
     * delta 每个段的增量数组键名 / 删除数组键名
     */
    public static final String KEY_UPSERTS = "upserts";
    public static final String KEY_DELETES = "deletes";
    /**
     * 实体信封字段：父容器内序号。delta 路径以它为准，服务端不做顺序归一化。
     */
    public static final String KEY_SORT_ORDER = "sortOrder";
    /**
     * delta 应用之后的完整 id / name 集合 —— 服务端据此校验归属与重命名。
     * 子分区 id 不在文件名里，服务端无法自行枚举，只能由客户端给出。
     */
    public static final String KEY_KNOWN_PAGE_IDS = "knownPageIds";
    public static final String KEY_KNOWN_OVERLAY_IDS = "knownOverlayIds";
    public static final String KEY_KNOWN_SECTION_IDS = "knownSectionIds";
    public static final String KEY_KNOWN_WIDGET_IDS = "knownWidgetIds";

    /**
     * 按 id 查页面（精确匹配，不做归一化 —— id 是前端生成的 UUID）。
     */
    public WorkshopPage findPageById(String pageId) {
        if (pages == null || StringUtils.isEmpty(pageId)) return null;
        return pages.stream()
                .filter(p -> pageId.equals(p.id))
                .findFirst()
                .orElse(null);
    }

    public WorkshopVariable findVariableByName(String variableName) {
        if (variables == null) return null;
        String normalized = WorkshopVariable.normalizeName(variableName);
        return variables.stream()
                .filter(v -> WorkshopVariable.normalizeName(v.getName()).equals(normalized))
                .findFirst()
                .orElse(null);
    }

    // Getters & Setters
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getCreatedBy() {
        return createdBy;
    }

    public void setCreatedBy(String createdBy) {
        this.createdBy = createdBy;
    }

    public LocalDateTime getCreatedAt() {
        return createdAt;
    }

    public void setCreatedAt(LocalDateTime createdAt) {
        this.createdAt = createdAt;
    }

    public String getUpdatedBy() {
        return updatedBy;
    }

    public void setUpdatedBy(String updatedBy) {
        this.updatedBy = updatedBy;
    }

    public LocalDateTime getUpdatedAt() {
        return updatedAt;
    }

    public void setUpdatedAt(LocalDateTime updatedAt) {
        this.updatedAt = updatedAt;
    }


    /**
     * Workshop Module CRUD 操作的 Descriptor 路由基类
     * <p>
     * 通过 PluginAction.doDescriptionProcess() 路由，遵从 OCP 原则：
     * 新的 Module 类型可以提供自己的 Descriptor 子类来定制行为，
     * 无需改动 tis-console 抽象层的 OntologyAction。
     * <p>
     * 支持的 type 值：get | list | update | delete | module-apply | module-save |
     * page-get | page-list | header-get | header-update
     * <p>
     * 使用方式（前端 URL）：
     * <pre>
     * POST /coredefine/corenodemanage.ajax
     *   ?event_submit_do_description_process=y
     *   &amp;action=plugin_action
     *   &amp;impl=com.qlangtech.tis.plugin.ontology.workshop.desc.WorkshopModuleOperationDesc$DftDesc
     *   &amp;type=create | get | list | update | delete
     *   &amp;ontologyDomainId={domainId}
     *   &amp;name={moduleName}
     *   &amp;description={description}
     * </pre>
     *
     * @see com.qlangtech.tis.extension.Descriptor#httpProcess(IControlMsgHandler, IPluginContext, Context)
     * @see com.qlangtech.tis.plugin.ontology.impl.infer.BasicInfterExecuteDesc 参考模式
     */
    @TISExtension
    public static class DefaultDescriptor extends OntologyDomainManipulate.BasicDesc implements DescriptorUseableShortComment {
        @Override
        public String getDisplayName() {
            return "Workshop";
        }

        @Override
        public EndType getEndType() {
            return EndType.OntologyWorkshop;
        }

        @Override
        public boolean isManipulateStorable() {
            return true;
        }

        @Override
        public String shortComment() {
            return "开启Workshop功能";
        }

        /**
         * 按 type 参数分派到 doUpdate / doDelete
         */
        @Override
        public final void httpProcess(IControlMsgHandler msgHandler,
                                      IPluginContext pluginContext,
                                      Context context) throws Exception {
            String type = msgHandler.getString("type");
            try {
                switch (type) {
                    case "get":
                        doGet(msgHandler, pluginContext, context);
                        break;
                    case "list":
                        doList(msgHandler, pluginContext, context);
                        break;
                    case "update":
                        doUpdate(msgHandler, pluginContext, context);
                        break;
                    case "delete":
                        doDelete(msgHandler, pluginContext, context);
                        break;
                    case "page-get":
                        doPageGet(msgHandler, pluginContext, context);
                        break;
                    case "page-list":
                        doPageList(msgHandler, pluginContext, context);
                        break;
                    case "module-save":
                        doSaveModule(msgHandler, pluginContext, context);
                        break;
                    case "module-apply":
                        doApplyModule(msgHandler, pluginContext, context);
                        break;
                    case "header-get":
                        doHeaderGet(msgHandler, pluginContext, context);
                        break;
                    case "header-update":
                        doHeaderUpdate(msgHandler, pluginContext, context);
                        break;
                    default:
                        pluginContext.addErrorMessage(context, "Unsupported workshop module operation type: " + type);
                }
            } catch (Exception e) {
                // 不要将异常吃掉，不然终端排查错误非常困难
                // pluginContext.addErrorMessage(context, "操作失败: " + e.getMessage());
                throw e;
            }
        }

        public static final WorkshopModuleService workshopModuleSvc = new WorkshopModuleService();

        /**
         * 子类通过此方法提供 WorkshopModuleService 实例（可覆盖以注入 Mock 或定制实现）
         */
        protected WorkshopModuleService createModuleService() {
            return workshopModuleSvc;
        }

        // ==================================================================
        //  具体操作
        // ==================================================================

//        private void doCreate(IControlMsgHandler msgHandler,
//                              IPluginContext pluginContext,
//                              Context context) {
//            String ontologyDomainId = msgHandler.getString("ontologyDomainId");
//            String moduleName = msgHandler.getString("name");
//            String description = msgHandler.getString("description");
//
//            if (StringUtils.isEmpty(ontologyDomainId) || StringUtils.isEmpty(moduleName)) {
//                //  pluginContext.addErrorMessage(context, "参数 'ontologyDomainId' 和 'name' 不能为空");
//                throw new IllegalStateException("param 'ontologyDomainId' and 'name' can not be empty");
//            }
//
//            String userName = pluginContext.getLoginUser().getName();
//            WorkshopModuleService svc = createModuleService();
//            WorkshopModule created = svc.createModule(moduleName, description, ontologyDomainId, userName);
//
//            JSONObject result = new JSONObject();
//            result.put(IAjaxResult.KEY_SUCCESS, true);
//            result.put("module", toResultJSON(created));
//            pluginContext.setBizResult(context, result);
//        }

        private void doGet(IControlMsgHandler msgHandler,
                           IPluginContext pluginContext,
                           Context context) throws Exception {
            String ontologyDomainId = msgHandler.getString("ontologyDomainId");
            String moduleName = msgHandler.getString("name");

            if (StringUtils.isEmpty(ontologyDomainId) || StringUtils.isEmpty(moduleName)) {
                throw new IllegalStateException("param 'ontologyDomainId' and 'name' can not be empty");
            }

            WorkshopModuleService svc = createModuleService();
            WorkshopModule module = svc.getModule(ontologyDomainId, moduleName);

            JSONObject result = new JSONObject();
            result.put(IAjaxResult.KEY_SUCCESS, true);
            JSONObject moduleJson = toResultJSON(module);

            // Load pages from independent storage (pages is transient)
            List<WorkshopPage> pages = module.loadAllPage();
            if (!pages.isEmpty()) {
                JSONArray pagesJson = new JSONArray();
                for (WorkshopPage p : pages) {
                    // 统一走 pageToJSON：它带 id（UUID 主键）。前端建 section 时
                    // section.parent.parentId 要写页面 id，这里漏下发 id 会让新 section 的归属悬空。
                    JSONObject pj = pageToJSON(p);
                    pj.put("moduleId", module.getId());
                    pagesJson.add(pj);
                }
                moduleJson.put("pages", pagesJson);
            }

            // header 同样来自独立存储（header 是 transient），但它与 pages 不一样、
            // 是单个对象而非数组，直接下发对象即可。
            // 不回填时前端 module-container 的 *ngIf="module.header?.visible" 恒为 false，
            // <app-workshop-header> 整块不渲染。
            if (module.header != null) {
                moduleJson.put("header", headerToJSON(module.header, moduleName));
            }

            // 变量同样来自独立存储（variables 是 transient），且下发 {impl, vals} 形态
            // 以便前端回填插件表单，因此不能像 pages 那样手写 JSON
            List<WorkshopVariable> variables = module.variables == null
                    ? Collections.emptyList() : module.variables;
            if (!variables.isEmpty()) {
                JSONArray variablesJson = new JSONArray();
                for (WorkshopVariable v : variables) {
                    JSONObject vj = WorkshopVariable.toVariableJSON(v);
                    vj.put("moduleId", module.getId());
                    variablesJson.add(vj);
                }
                moduleJson.put("variables", variablesJson);
            }

            // overlays / sections / widgets 同理来自独立存储（模块 XML 里只有 module 自己的字段）。
            // 一律下发 GET 形态的 item JSON（{impl, displayName, vals}），前端据此重建可编辑的 Item；
            // 归属关系不是表单字段，因此作为信封字段与 vals 平级下发。
            attachLayoutJSON(module, moduleJson);

            result.put("module", moduleJson);
            pluginContext.setBizResult(context, result);
        }

        /**
         * 把模块加载后的树（page.sections / overlay.sections / section.widgets）投影成 GET 形态 JSON。
         * <p>
         * 以「树」而不是「存储目录」为准：孤立无主的 section / widget（父容器已删）不在这里出现。
         * 增量保存（{@code module-apply}）下它们由客户端的显式 delete 列表清理；
         * 只有回退到整对象快照（{@code module-save}）时才按快照差集删除。
         */
        private static void attachLayoutJSON(WorkshopModule module, JSONObject moduleJson) throws Exception {
            List<WorkshopOverlay> overlays = module.overlays == null
                    ? Collections.emptyList() : module.overlays;
            JSONArray overlaysJson = new JSONArray();
            for (WorkshopOverlay overlay : overlays) {
                overlaysJson.add(new DescribableJSON<>(overlay).getItemJson());
            }
            moduleJson.put(KEY_OVERLAYS, overlaysJson);

            List<WorkshopSection> topSections = new ArrayList<>();
            if (module.pages != null) {
                for (WorkshopPage page : module.pages) {
                    if (page.sections != null) {
                        topSections.addAll(page.sections);
                    }
                }
            }
            for (WorkshopOverlay overlay : overlays) {
                if (overlay.sections != null) {
                    topSections.addAll(overlay.sections);
                }
            }

            JSONArray sectionsJson = new JSONArray();
            JSONArray widgetsJson = new JSONArray();
            for (WorkshopSection section : topSections) {
                JSONObject sectionJson = new DescribableJSON<>(section).getItemJson();
                WorkshopCptParentId parent = Objects.requireNonNull(section.parent,
                        "section " + section.identityValue() + " parent can not be null");
                sectionJson.put(KEY_PARENT_ID, parent.parentId);
                sectionJson.put(KEY_PARENT_TYPE, parent.parentType.getLiteral());
                sectionsJson.add(sectionJson);

                appendWidgetsJSON(section, widgetsJson);
                // 子分区本身嵌套在父 section 的 childSections 里，但它挂的 widget 同样是
                // 独立落盘的实体 —— 漏下发会让这些 widget 在前端消失，且下一次快照保存时
                // 被差集删除。
                if (section.childSections != null) {
                    for (SectionChild child : section.childSections) {
                        appendWidgetsJSON(child, widgetsJson);
                    }
                }
            }
            moduleJson.put(KEY_SECTIONS, sectionsJson);
            moduleJson.put(KEY_WIDGETS, widgetsJson);
        }

        /**
         * 把一个 section（顶层或子分区）的 widget 追加到下发数组，并带上归属信封字段。
         */
        private static void appendWidgetsJSON(AbstractWorkshopSection section, JSONArray widgetsJson)
                throws Exception {
            if (section.widgets == null) {
                return;
            }
            for (WorkshopWidget widget : section.widgets) {
                JSONObject widgetJson = widgetToJSON(widget);
                widgetJson.put(KEY_SECTION_ID, widget.sectionId);
                widgetsJson.add(widgetJson);
            }
        }

        /**
         * widget 的 item JSON。
         * <p>
         * {@link WorkshopWidget} 的扩展点类型是 {@code IWorkshopWidget}（即
         * {@code Describable<IWorkshopWidget>}），不满足 {@link DescribableJSON} 的
         * {@code T extends Describable<T>} 约束，故这里用裸类型构造 —— 运行期行为与正常路径一致。
         */
        @SuppressWarnings({"rawtypes", "unchecked"})
        private static JSONObject widgetToJSON(WorkshopWidget widget) throws Exception {
            return new DescribableJSON(widget, widget.getDescriptor()).getItemJson();
        }

        private void doList(IControlMsgHandler msgHandler,
                            IPluginContext pluginContext,
                            Context context) {
            String ontologyDomainId = msgHandler.getString("ontologyDomainId");

            if (StringUtils.isEmpty(ontologyDomainId)) {
//                pluginContext.addErrorMessage(context, "参数 'ontologyDomainId' 不能为空");
//                return;
                throw new IllegalStateException("param 'ontologyDomainId'  can not be empty");
            }

            WorkshopModuleService svc = createModuleService();
            List<WorkshopModule> modules = svc.listModules(ontologyDomainId);

            List<JSONObject> jsonList = modules.stream()
                    .map(DefaultDescriptor::toResultJSON)
                    .collect(Collectors.toList());

            JSONObject result = new JSONObject();
            result.put(IAjaxResult.KEY_SUCCESS, true);
            result.put("modules", new JSONArray(jsonList));
            result.put("total", jsonList.size());
            pluginContext.setBizResult(context, result);
        }

        private void doUpdate(IControlMsgHandler msgHandler,
                              IPluginContext pluginContext,
                              Context context) {
            String ontologyDomainId = msgHandler.getString("ontologyDomainId");
            String moduleName = msgHandler.getString("name");
            String description = msgHandler.getString("description");

            if (StringUtils.isEmpty(ontologyDomainId) || StringUtils.isEmpty(moduleName)) {
//                pluginContext.addErrorMessage(context, "参数 'ontologyDomainId' 和 'name' 不能为空");
//                return;
                throw new IllegalStateException("param 'ontologyDomainId' and 'name' can not be empty");
            }

            WorkshopModuleService svc = createModuleService();
            WorkshopModule module = svc.getModule(ontologyDomainId, moduleName);

            if (StringUtils.isNotEmpty(description)) {
                module.description = description;
            }

            String userName = pluginContext.getLoginUser().getName();
            WorkshopModule updated = svc.updateModule(ontologyDomainId, moduleName, module, userName);

            JSONObject result = new JSONObject();
            result.put(IAjaxResult.KEY_SUCCESS, true);
            result.put("module", toResultJSON(updated));
            pluginContext.setBizResult(context, result);
        }

        private void doDelete(IControlMsgHandler msgHandler,
                              IPluginContext pluginContext,
                              Context context) {
            String ontologyDomainId = msgHandler.getString("ontologyDomainId");
            String moduleName = msgHandler.getString("name");

            if (StringUtils.isEmpty(ontologyDomainId) || StringUtils.isEmpty(moduleName)) {
//                pluginContext.addErrorMessage(context, "参数 'ontologyDomainId' 和 'name' 不能为空");
//                return;
                throw new IllegalStateException("param 'ontologyDomainId' and 'name' can not be empty");
            }

            WorkshopModuleService svc = createModuleService();
            svc.deleteModule(ontologyDomainId, moduleName);

            JSONObject result = new JSONObject();
            result.put(IAjaxResult.KEY_SUCCESS, true);
            result.put("message", "Workshop Module 已删除");
            pluginContext.setBizResult(context, result);
        }

        // ==================================================================
        //  WorkshopPage 独立存储
        //
        //  读写全部委托给 WorkshopPageStore（存储路径
        //  ontology/{domain}/workshop/{moduleName}/pages/{pageId}.xml）。
        //  页面属于模块，故所有读写都必须带 moduleName —— domain 级列表没有意义。
        //
        //  写入入口：module-apply（增量，主路径，只写变化的页面 + 显式删除）与
        //  module-save（无增量基准时的兜底整对象快照，按 id 覆盖写并删除快照中不存在的
        //  文件）。不再提供逐字段的 page-update —— 字段 patch 需要服务端
        //  read-modify-write，合并顺序与并发都不可控。
        // ==================================================================

        /**
         * page-get: 查询单个 Page
         */
        private void doPageGet(IControlMsgHandler msgHandler,
                               IPluginContext pluginContext,
                               Context context) {
            String ontologyDomainId = msgHandler.getString("ontologyDomainId");
            String moduleName = msgHandler.getString("name");
            String pageId = msgHandler.getString(PARAM_PAGE_ID);

            if (StringUtils.isEmpty(ontologyDomainId) || StringUtils.isEmpty(moduleName)
                    || StringUtils.isEmpty(pageId)) {
                throw new IllegalStateException("param 'ontologyDomainId', 'name' and '"
                        + PARAM_PAGE_ID + "' can not be empty");
            }
            WorkshopPage page = WorkshopPage.loadPage(ontologyDomainId, moduleName, pageId);
            if (page == null) {
                pluginContext.addErrorMessage(context, "Page not found: " + pageId);
                return;
            }

            JSONObject result = new JSONObject();
            result.put(IAjaxResult.KEY_SUCCESS, true);
            result.put("page", pageToJSON(page));
            pluginContext.setBizResult(context, result);
        }

        /**
         * page-list: 列出模块下的所有 Page
         */
        private void doPageList(IControlMsgHandler msgHandler,
                                IPluginContext pluginContext,
                                Context context) {
            String ontologyDomainId = msgHandler.getString("ontologyDomainId");
            String moduleName = msgHandler.getString("name");

            if (StringUtils.isEmpty(ontologyDomainId) || StringUtils.isEmpty(moduleName)) {
                throw new IllegalStateException("param 'ontologyDomainId' and 'name' can not be empty");
            }

            List<WorkshopPage> pages = WorkshopPage.loadAll(ontologyDomainId, moduleName);

            JSONArray pagesJson = new JSONArray();
            for (WorkshopPage p : pages) {
                pagesJson.add(pageToJSON(p));
            }

            JSONObject result = new JSONObject();
            result.put(IAjaxResult.KEY_SUCCESS, true);
            result.put("pages", pagesJson);
            result.put("total", pagesJson.size());
            pluginContext.setBizResult(context, result);
        }

        /**
         * module-save: 以客户端快照覆盖保存模块子实体。
         *
         * <p>快照包含四类实体，每一类都是<b>完整对象</b>、以其自身数组顺序表达同层顺序：
         * <ul>
         *   <li>{@code pages}：{@code item.project()} 投影后的 POST 项，另带信封 {@code sortOrder}</li>
         *   <li>{@code overlays}：同上</li>
         *   <li>{@code sections}：同上，另带信封字段 {@code parentId} / {@code parentType}；
         *       子分区嵌在各自 {@code childSections} 字段里，不单独出现</li>
         *   <li>{@code widgets}：同上，另带信封字段 {@code sectionId}</li>
         * </ul>
         * 四类实体统一由 {@link WorkshopFormParser} 解析并校验（{@code AttrValMap} +
         * {@code validateWithScope(STRICT)} + {@code newInstance()}），与配置面板提交的表单
         * 走同一条链路。服务端不做字段合并，逐实体覆盖写，再删掉快照里已不存在的文件。
         * section / overlay / widget 的写盘由实例自己完成
         * （{@link IStoreSelfManagement#manipuldateProcess}），与标准 {@code plugin_action}
         * 保存链路共用同一条路径。
         *
         * <p>请求示例（路由参数在 query string，快照是 <b>JSON body</b>）：
         * <pre>
         *   POST ...?type=module-save&amp;ontologyDomainId=xxx&amp;name=moduleName
         *   body: {"pages":[...],"overlays":[...],"sections":[...],"widgets":[...]}
         * </pre>
         * {@code sortOrder} 以数组顺序为准（服务端按父容器归一化成 0..n-1），前端传来的值只作参考。
         */
        private void doSaveModule(IControlMsgHandler msgHandler,
                                  IPluginContext pluginContext,
                                  Context context) {
            String ontologyDomainId = msgHandler.getString("ontologyDomainId");
            String moduleName = msgHandler.getString("name");

            if (StringUtils.isEmpty(ontologyDomainId) || StringUtils.isEmpty(moduleName)) {
                throw new IllegalStateException("param 'ontologyDomainId' and 'name' can not be empty");
            }

            JSONObject snapshot = pluginContext.getJSONPostContent();
            if (snapshot == null || !snapshot.containsKey(KEY_PAGES)) {
                throw new IllegalStateException("json body must contain '" + KEY_PAGES + "'");
            }

            // ---- 解析与校验：四段全部完成之后才开始写盘 ----
            // 任何一段不合法（必填 / 枚举 / 子表单）都会在这里抛错，磁盘不会留下半写状态。
            // 页面先解析：section 的 parentId 校验以「快照里的页面」为准，数组顺序也决定页面序号。
            List<WorkshopPage> pages = parsePages(msgHandler, context,
                    snapshot.getJSONArray(KEY_PAGES), true);

            List<WorkshopOverlay> overlays = parseOverlays(msgHandler, context,
                    snapshot.getJSONArray(KEY_OVERLAYS), false);
            // 数组顺序即浮层顺序：排序键取常量，normalizeOrder 的稳定排序保持数组原序
            normalizeEntityOrder(overlays, o -> 0, (o, index) -> o.sortOrder = index);

            List<WorkshopSection> sections = parseSections(msgHandler, context,
                    snapshot.getJSONArray(KEY_SECTIONS), identities(pages), identities(overlays), false);
            normalizeSectionOrder(sections);

            List<WorkshopWidget> widgets = parseWidgets(msgHandler, context,
                    snapshot.getJSONArray(KEY_WIDGETS), collectSectionIds(sections), false);
            normalizeWidgetOrder(widgets);

            // ---- 写盘 ----
            WorkshopPageStore pageStore = WorkshopPageStore.create(ontologyDomainId, moduleName);
            Set<String> removedPages = pageStore.replaceAll(pluginContext, Optional.of(context), pages);

            UploadPluginMeta selfManagementMeta =
                    AbstractWorkshopEntityStore.createSelfManagementMeta(ontologyDomainId, moduleName);

            persistEntities(overlays, selfManagementMeta, pluginContext, context);
            Set<String> removedOverlays = WorkshopOverlayStore.create(ontologyDomainId, moduleName)
                    .deleteNotIn(identities(overlays));

            persistEntities(sections, selfManagementMeta, pluginContext, context);
            Set<String> removedSections = WorkshopSectionStore.create(ontologyDomainId, moduleName)
                    .deleteNotIn(identities(sections));

            persistEntities(widgets, selfManagementMeta, pluginContext, context);
            Set<String> removedWidgets = WorkshopWidgetStore.create(ontologyDomainId, moduleName)
                    .deleteNotIn(identities(widgets));

            // 各 store 的 save/delete 已各自递增 generation，这里再兜一次底：
            // 即便将来某条写路径漏了失效，整模块保存后缓存也一定会重建
            invalidateWorkshopData(ontologyDomainId, moduleName);

            JSONObject result = new JSONObject();
            result.put(IAjaxResult.KEY_SUCCESS, true);
            result.put("savedCount", pages.size());
            result.put("removedCount", removedPages.size());
            result.put(KEY_PAGES + "Count", pages.size());
            result.put(KEY_OVERLAYS + "Count", overlays.size());
            result.put(KEY_SECTIONS + "Count", sections.size());
            result.put(KEY_WIDGETS + "Count", widgets.size());
            result.put("removed" + StringUtils.capitalize(KEY_OVERLAYS), removedOverlays.size());
            result.put("removed" + StringUtils.capitalize(KEY_SECTIONS), removedSections.size());
            result.put("removed" + StringUtils.capitalize(KEY_WIDGETS), removedWidgets.size());
            JSONArray pagesJson = new JSONArray();
            for (WorkshopPage p : pageStore.listAll()) {
                pagesJson.add(pageToJSON(p));
            }
            result.put(KEY_PAGES, pagesJson);
            pluginContext.setBizResult(context, result);
        }

        /**
         * module-apply: 增量提交 —— 只写本次编辑过的实体。
         *
         * <p>与 {@link #doSaveModule} 的整对象快照不同，这里<b>不做差集删除</b>：删除必须由
         * 客户端显式放进各段的 {@code deletes}。这样服务端写盘量与前端 diff 出来的改动量一致，
         * 也避免「快照里漏了一个实体，它的文件就被静默删掉」这类整对象提交固有的风险。
         *
         * <p>写入顺序（upsert pages → overlays → sections → widgets；delete 反向）保证：
         * 重命名时新实体先存在、删除时子实体先于父实体消失。
         *
         * <p>所有解析与校验都在写盘之前完成 —— 请求被拒绝时磁盘不发生变化。
         *
         * <p>路由参数（{@code ontologyDomainId} / {@code name}）在 query string，
         * delta 是 <b>JSON body</b>：载荷里有多层嵌套的 {@code descVal.vals}，走 body 才能由
         * {@code getJSONPostContent()} 以 {@code Feature.OrderedField} 解析，保住
         * MULTI_DESCRIBLE_PLUGIN 记录的行顺序（塞进 query 参数则只能拿到被打散的对象）。
         */
        private void doApplyModule(IControlMsgHandler msgHandler,
                                   IPluginContext pluginContext,
                                   Context context) {
            String ontologyDomainId = msgHandler.getString("ontologyDomainId");
            String moduleName = msgHandler.getString("name");

            if (StringUtils.isEmpty(ontologyDomainId) || StringUtils.isEmpty(moduleName)) {
                throw new IllegalStateException("param 'ontologyDomainId' and 'name' can not be empty");
            }

            JSONObject delta = pluginContext.getJSONPostContent();
            if (delta == null) {
                throw new IllegalStateException("json body must be a delta object");
            }

            // known* 是「应用 delta 之后」的完整集合，不是基准、也不只是 upserts：
            // 子分区 id 不在文件名里，服务端无法自行枚举，只能由客户端给出。
            Set<String> knownPageIds = readKnownSet(delta, KEY_KNOWN_PAGE_IDS);
            Set<String> knownOverlayIds = readKnownSet(delta, KEY_KNOWN_OVERLAY_IDS);
            Set<String> knownSectionIds = readKnownSet(delta, KEY_KNOWN_SECTION_IDS);
            Set<String> knownWidgetIds = readKnownSet(delta, KEY_KNOWN_WIDGET_IDS);

            JSONObject pagesDelta = delta.getJSONObject(KEY_PAGES);
            List<WorkshopPage> pageUpserts = parsePages(msgHandler, context,
                    readUpserts(pagesDelta), false);
            Set<String> pageDeletes = readDeletes(pagesDelta);

            JSONObject overlaysDelta = delta.getJSONObject(KEY_OVERLAYS);
            List<WorkshopOverlay> overlayUpserts =
                    parseOverlays(msgHandler, context, readUpserts(overlaysDelta), true);
            Set<String> overlayDeletes = readDeletes(overlaysDelta);

            JSONObject sectionsDelta = delta.getJSONObject(KEY_SECTIONS);
            List<WorkshopSection> sectionUpserts = parseSections(msgHandler, context,
                    readUpserts(sectionsDelta), knownPageIds, knownOverlayIds, true);
            Set<String> sectionDeletes = readDeletes(sectionsDelta);

            JSONObject widgetsDelta = delta.getJSONObject(KEY_WIDGETS);
            List<WorkshopWidget> widgetUpserts = parseWidgets(msgHandler, context,
                    readUpserts(widgetsDelta), knownSectionIds, true);
            Set<String> widgetDeletes = readDeletes(widgetsDelta);

            // ---- 校验（全部在写盘之前） ----
            Set<String> pageUpsertIds = identities(pageUpserts);
            Set<String> overlayUpsertIds = identities(overlayUpserts);
            Set<String> sectionUpsertIds = identities(sectionUpserts);
            Set<String> widgetUpsertIds = identities(widgetUpserts);

            assertUpsertsKnown(pageUpsertIds, knownPageIds, "page");
            assertUpsertsKnown(overlayUpsertIds, knownOverlayIds, "overlay");
            assertUpsertsKnown(sectionUpsertIds, knownSectionIds, "section");
            assertUpsertsKnown(widgetUpsertIds, knownWidgetIds, "widget");

            assertDeletesValid(pageDeletes, pageUpsertIds, knownPageIds, "page");
            assertDeletesValid(overlayDeletes, overlayUpsertIds, knownOverlayIds, "overlay");
            assertDeletesValid(sectionDeletes, sectionUpsertIds, knownSectionIds, "section");
            assertDeletesValid(widgetDeletes, widgetUpsertIds, knownWidgetIds, "widget");

            // 子分区随父 section 落盘：它们的 id 也必须在 knownSectionIds 里，
            // 否则该父 section 下的 widget 归属会悬空。
            for (WorkshopSection section : sectionUpserts) {
                if (section.childSections == null) {
                    continue;
                }
                for (SectionChild child : section.childSections) {
                    if (!knownSectionIds.contains(child.identityValue())) {
                        throw new IllegalStateException("child section '" + child.identityValue()
                                + "' of section '" + section.identityValue()
                                + "' is not in delta '" + KEY_KNOWN_SECTION_IDS + "'");
                    }
                }
            }

            WorkshopPageStore pageStore = WorkshopPageStore.create(ontologyDomainId, moduleName);
            WorkshopOverlayStore overlayStore = WorkshopOverlayStore.create(ontologyDomainId, moduleName);
            WorkshopSectionStore sectionStore = WorkshopSectionStore.create(ontologyDomainId, moduleName);
            WorkshopWidgetStore widgetStore = WorkshopWidgetStore.create(ontologyDomainId, moduleName);

            // 路径安全校验（含 "../" 穿越）：在写任何文件之前把非法 id 挡下来
            for (String pageId : pageDeletes) {
                pageStore.getPageFile(pageId);
            }
            for (String overlayId : overlayDeletes) {
                overlayStore.getEntityFile(overlayId);
            }
            for (String sectionId : sectionDeletes) {
                sectionStore.getEntityFile(sectionId);
            }
            for (String widgetId : widgetDeletes) {
                widgetStore.getEntityFile(widgetId);
            }

            // 只有发生删除时才做孤儿检查：把「客户端漏发子实体 delete」暴露在写入之前。
            if (!pageDeletes.isEmpty() || !overlayDeletes.isEmpty()
                    || !sectionDeletes.isEmpty() || !widgetDeletes.isEmpty()) {
                assertNoOrphans(ontologyDomainId, moduleName, pageDeletes, overlayDeletes,
                        sectionDeletes, widgetDeletes, sectionUpserts);
            }

            // ---- 写入：先 upsert（父 → 子），再 delete（子 → 父） ----
            UploadPluginMeta selfManagementMeta =
                    AbstractWorkshopEntityStore.createSelfManagementMeta(ontologyDomainId, moduleName);

            for (WorkshopPage page : pageUpserts) {
                pageStore.save(pluginContext, Optional.of(context), page);
            }
            persistEntities(overlayUpserts, selfManagementMeta, pluginContext, context);
            persistEntities(sectionUpserts, selfManagementMeta, pluginContext, context);
            persistEntities(widgetUpserts, selfManagementMeta, pluginContext, context);

            for (String widgetId : widgetDeletes) {
                widgetStore.delete(pluginContext, Optional.of(context), widgetId);
            }
            for (String sectionId : sectionDeletes) {
                sectionStore.delete(pluginContext, Optional.of(context), sectionId);
            }
            for (String overlayId : overlayDeletes) {
                overlayStore.delete(pluginContext, Optional.of(context), overlayId);
            }
            for (String pageId : pageDeletes) {
                pageStore.delete(pluginContext, Optional.of(context), pageId);
            }

            // 与 doSaveModule 一致：各 store 已递增，这里兜底保证整批 delta 后必然重建
            invalidateWorkshopData(ontologyDomainId, moduleName);

            JSONObject result = new JSONObject();
            result.put(IAjaxResult.KEY_SUCCESS, true);
            result.put("upserted", counts(pageUpserts.size(), overlayUpserts.size(),
                    sectionUpserts.size(), widgetUpserts.size()));
            result.put("deleted", counts(pageDeletes.size(), overlayDeletes.size(),
                    sectionDeletes.size(), widgetDeletes.size()));
            pluginContext.setBizResult(context, result);
        }

        private static JSONObject counts(int pages, int overlays, int sections, int widgets) {
            JSONObject json = new JSONObject();
            json.put(KEY_PAGES, pages);
            json.put(KEY_OVERLAYS, overlays);
            json.put(KEY_SECTIONS, sections);
            json.put(KEY_WIDGETS, widgets);
            return json;
        }

        private static JSONArray readUpserts(JSONObject segment) {
            if (segment == null) {
                return new JSONArray();
            }
            JSONArray upserts = segment.getJSONArray(KEY_UPSERTS);
            return upserts == null ? new JSONArray() : upserts;
        }

        private static Set<String> readDeletes(JSONObject segment) {
            Set<String> deletes = new HashSet<>();
            if (segment == null) {
                return deletes;
            }
            JSONArray json = segment.getJSONArray(KEY_DELETES);
            if (json == null) {
                return deletes;
            }
            for (int i = 0; i < json.size(); i++) {
                String value = StringUtils.trimToNull(json.getString(i));
                if (value == null) {
                    throw new IllegalStateException("delta '" + KEY_DELETES + "[" + i + "]' can not be empty");
                }
                if (!deletes.add(value)) {
                    throw new IllegalStateException("delta '" + KEY_DELETES + "' contains duplicate: " + value);
                }
            }
            return deletes;
        }

        private static Set<String> readKnownSet(JSONObject delta, String key) {
            JSONArray json = delta.getJSONArray(key);
            Set<String> result = new HashSet<>();
            if (json == null) {
                return result;
            }
            for (int i = 0; i < json.size(); i++) {
                String value = StringUtils.trimToNull(json.getString(i));
                if (value == null) {
                    throw new IllegalStateException("delta '" + key + "[" + i + "]' can not be empty");
                }
                if (!result.add(value)) {
                    throw new IllegalStateException("delta '" + key + "' contains duplicate: " + value);
                }
            }
            return result;
        }

        private static void assertUpsertsKnown(Set<String> upserts, Set<String> known, String label) {
            for (String identity : upserts) {
                if (!known.contains(identity)) {
                    throw new IllegalStateException(label + " '" + identity
                            + "' is not in the post-delta known set (client and server out of sync)");
                }
            }
        }

        private static void assertDeletesValid(Set<String> deletes, Set<String> upserts,
                                               Set<String> known, String label) {
            for (String identity : deletes) {
                if (upserts.contains(identity)) {
                    throw new IllegalStateException(label + " '" + identity
                            + "' can not be upserted and deleted in the same delta");
                }
                if (known.contains(identity)) {
                    throw new IllegalStateException(label + " '" + identity
                            + "' is marked as deleted but still exists in the post-delta known set");
                }
            }
        }

        /**
         * 引用完整性检查：删除父实体时必须同时删除其子实体。
         *
         * <p>只检查「对被删实体的残留引用」，不检查历史孤儿 —— 增量保存不再做差集删除，
         * 旧的孤儿文件会一直留着（需要时另做一次维护型全量对账）。
         */
        private static void assertNoOrphans(String ontologyDomainId, String moduleName,
                                            Set<String> pageDeletes, Set<String> overlayDeletes,
                                            Set<String> sectionDeletes, Set<String> widgetDeletes,
                                            List<WorkshopSection> sectionUpserts) {
            List<WorkshopSection> storedSections = WorkshopSectionStore.create(
                    ontologyDomainId, moduleName).listAll();
            List<WorkshopWidget> storedWidgets = WorkshopWidgetStore.create(
                    ontologyDomainId, moduleName).listAll();

            // 被删 section 连同其子分区一起消失；父 section 被 upsert 时，旧子分区里
            // 没有出现在新数组中的记录同样消失。
            Set<String> vanishedSectionIds = new HashSet<>(sectionDeletes);
            Map<String, Set<String>> upsertChildIds = new LinkedHashMap<>();
            for (WorkshopSection section : sectionUpserts) {
                Set<String> childIds = new HashSet<>();
                if (section.childSections != null) {
                    for (SectionChild child : section.childSections) {
                        childIds.add(child.identityValue());
                    }
                }
                upsertChildIds.put(section.identityValue(), childIds);
            }
            for (WorkshopSection stored : storedSections) {
                String sectionId = stored.identityValue();
                Set<String> newChildIds = upsertChildIds.get(sectionId);
                if (!sectionDeletes.contains(sectionId) && newChildIds == null) {
                    continue;
                }
                if (stored.childSections == null) {
                    continue;
                }
                for (SectionChild child : stored.childSections) {
                    if (newChildIds == null || !newChildIds.contains(child.identityValue())) {
                        vanishedSectionIds.add(child.identityValue());
                    }
                }
            }

            for (WorkshopWidget widget : storedWidgets) {
                if (widgetDeletes.contains(widget.identityValue())) {
                    continue;
                }
                if (widget.sectionId != null && vanishedSectionIds.contains(widget.sectionId)) {
                    throw new IllegalStateException("widget '" + widget.identityValue()
                            + "' still refers to removed section '" + widget.sectionId
                            + "'; include it in delta '" + KEY_WIDGETS + "." + KEY_DELETES + "'");
                }
            }

            Set<String> sectionUpsertIds = upsertChildIds.keySet();
            for (WorkshopSection section : storedSections) {
                String sectionId = section.identityValue();
                if (sectionDeletes.contains(sectionId) || sectionUpsertIds.contains(sectionId)
                        || section.parent == null) {
                    continue;
                }
                if (section.parent.parentType == SectionParentType.PAGE
                        && pageDeletes.contains(section.parent.parentId)) {
                    throw new IllegalStateException("section '" + sectionId
                            + "' still refers to removed page '" + section.parent.parentId
                            + "'; include it in delta '" + KEY_SECTIONS + "." + KEY_DELETES + "'");
                }
                if (section.parent.parentType == SectionParentType.OVERLAY
                        && overlayDeletes.contains(section.parent.parentId)) {
                    throw new IllegalStateException("section '" + sectionId
                            + "' still refers to removed overlay '" + section.parent.parentId
                            + "'; include it in delta '" + KEY_SECTIONS + "." + KEY_DELETES + "'");
                }
            }
        }

        /**
         * 逐实体调用 {@link IStoreSelfManagement#manipuldateProcess} 完成落盘 ——
         * 这是「实例自管理」链路在快照保存里的入口，存储位置由元数据里的 domain + module 决定。
         */
        private static <T extends Describable<?> & IdentityName & IStoreSelfManagement> void persistEntities(
                List<T> entities, UploadPluginMeta selfManagementMeta,
                IPluginContext pluginContext, Context context) {
            for (T entity : entities) {
                entity.manipuldateProcess(pluginContext, selfManagementMeta, Optional.of(context));
            }
        }

        private static <T extends IdentityName> Set<String> identities(List<T> entities) {
            Set<String> ids = new HashSet<>();
            for (T entity : entities) {
                ids.add(entity.identityValue());
            }
            return ids;
        }

        /**
         * 按快照数组顺序压实同层序号。{@code sortOrder} 的语义是「父容器内序号」，
         * 因此只对同一个父容器内的兄弟做归一化。
         */
        private static <T> void normalizeEntityOrder(List<T> entities,
                                                     java.util.function.ToIntFunction<T> sortOrder,
                                                     java.util.function.ObjIntConsumer<T> setter) {
            AbstractWorkshopSection.normalizeOrder(entities, sortOrder, setter);
        }

        /**
         * section 按父容器（{@code parentType + parentId}）分组后各自压实为 0..n-1。
         */
        private static void normalizeSectionOrder(List<WorkshopSection> sections) {
            Map<String, List<WorkshopSection>> grouped = new LinkedHashMap<>();
            for (WorkshopSection section : sections) {
                String groupKey = section.parent.parentType.getLiteral() + ":" + section.parent.parentId;
                grouped.computeIfAbsent(groupKey, k -> new ArrayList<>()).add(section);
            }
            for (List<WorkshopSection> group : grouped.values()) {
                normalizeEntityOrder(group, s -> 0, (s, index) -> s.sortOrder = index);
            }
        }

        /**
         * widget 按所属 section 分组后各自压实为 0..n-1。
         */
        private static void normalizeWidgetOrder(List<WorkshopWidget> widgets) {
            Map<String, List<WorkshopWidget>> grouped = new LinkedHashMap<>();
            for (WorkshopWidget widget : widgets) {
                grouped.computeIfAbsent(widget.sectionId, k -> new ArrayList<>()).add(widget);
            }
            for (List<WorkshopWidget> group : grouped.values()) {
                normalizeEntityOrder(group, w -> 0, (w, index) -> w.sortOrder = index);
            }
        }

        /**
         * 解析快照里的 overlay 数组。元素是 POST 形态的 item（{@code {impl, vals}}）。
         */
        private static List<WorkshopOverlay> parseOverlays(IControlMsgHandler msgHandler, Context context,
                                                           JSONArray overlaysJson,
                                                           boolean sortOrderFromEnvelope) {
            List<WorkshopOverlay> overlays = new ArrayList<>();
            if (overlaysJson == null) {
                return overlays;
            }
            Set<String> ids = new HashSet<>();
            for (int i = 0; i < overlaysJson.size(); i++) {
                JSONObject itemJson = overlaysJson.getJSONObject(i);
                WorkshopOverlay overlay = parseEntity(msgHandler, context,
                        itemJson, "overlay[" + i + "]", WorkshopOverlay.class);
                if (!ids.add(overlay.identityValue())) {
                    throw new IllegalStateException("duplicate overlay id: " + overlay.identityValue());
                }
                if (sortOrderFromEnvelope) {
                    // delta 路径不做顺序归一化：序号是客户端当前实体上的权威值
                    overlay.sortOrder = readEnvelopeSortOrder(itemJson, "overlay[" + overlay.identityValue() + "]");
                }
                overlays.add(overlay);
            }
            return overlays;
        }

        /**
         * 读取实体信封上的 {@code sortOrder}。delta 路径要求它必填 —— 服务端不再按数组
         * 顺序重新归一化，缺失或非法会让排序静默错乱。
         */
        private static int readEnvelopeSortOrder(JSONObject itemJson, String label) {
            Integer sortOrder = itemJson.getInteger(KEY_SORT_ORDER);
            if (sortOrder == null || sortOrder < 0) {
                throw new IllegalStateException(label + " envelope '" + KEY_SORT_ORDER
                        + "' must be a non-negative integer");
            }
            return sortOrder;
        }

        /**
         * 解析快照里的顶层 section 数组，并从信封字段回填归属（{@code parent}）。
         * <p>
         * {@code parent} 不是表单字段，前端不编辑它；归属由快照信封携带、在这里回填，
         * 然后由 {@link AbstractWorkshopSection#beforeSaved} 校验非空。
         */
        private static List<WorkshopSection> parseSections(IControlMsgHandler msgHandler, Context context,
                                                           JSONArray sectionsJson,
                                                           Set<String> pageIds,
                                                           Set<String> overlayIds,
                                                           boolean sortOrderFromEnvelope) {
            List<WorkshopSection> sections = new ArrayList<>();
            if (sectionsJson == null) {
                return sections;
            }
            Set<String> sectionIds = new HashSet<>();
            for (int i = 0; i < sectionsJson.size(); i++) {
                JSONObject itemJson = sectionsJson.getJSONObject(i);
                WorkshopSection section = parseEntity(msgHandler, context,
                        itemJson, "section[" + i + "]", WorkshopSection.class);
                if (sortOrderFromEnvelope) {
                    section.sortOrder = readEnvelopeSortOrder(itemJson,
                            "section[" + section.identityValue() + "]");
                }
                String parentId = StringUtils.trimToNull(itemJson.getString(KEY_PARENT_ID));
                String parentType = StringUtils.trimToNull(itemJson.getString(KEY_PARENT_TYPE));
                if (parentId == null || parentType == null) {
                    throw new IllegalStateException("section[" + section.identityValue() + "] envelope '"
                            + KEY_PARENT_ID + "' and '" + KEY_PARENT_TYPE + "' can not be empty");
                }
                SectionParentType type;
                try {
                    type = SectionParentType.parse(parentType);
                } catch (IllegalArgumentException e) {
                    throw new IllegalStateException("section[" + section.identityValue() + "] " + e.getMessage(), e);
                }
                switch (type) {
                    case PAGE -> {
                        if (!pageIds.contains(parentId)) {
                            throw new IllegalStateException("section[" + section.identityValue()
                                    + "] refers to unknown page: " + parentId);
                        }
                    }
                    case OVERLAY -> {
                        if (!overlayIds.contains(parentId)) {
                            throw new IllegalStateException("section[" + section.identityValue()
                                    + "] refers to unknown overlay: " + parentId);
                        }
                    }
                    case SECTION -> throw new IllegalStateException("section[" + section.identityValue()
                            + "] is a top-level section, parentType 'section' is not allowed in snapshot '"
                            + KEY_SECTIONS + "'; child sections belong to their parent's childSections");
                }
                section.parent = new WorkshopCptParentId(parentId, type);
                if (!sectionIds.add(section.identityValue())) {
                    throw new IllegalStateException("duplicate section id: " + section.identityValue());
                }
                sections.add(section);
            }
            return sections;
        }

        /**
         * 解析快照里的 widget 数组，并从信封字段回填 {@link WorkshopWidget#sectionId}。
         * <p>
         * 可挂载的 section 包括顶层 section 与它们的子分区（{@code childSections}）——
         * 子分区随父 section 落盘，但 widget 是按 {@code sectionId} 独立落盘的。
         */
        /**
         * 可挂载 section 的 id 集合 —— 顶层 section 与其子分区（子分区随父 section 落盘，
         * 但 widget 是按 {@code sectionId} 独立落盘的）。
         */
        private static Set<String> collectSectionIds(List<WorkshopSection> sections) {
            Set<String> sectionIds = new HashSet<>();
            for (WorkshopSection section : sections) {
                sectionIds.add(section.identityValue());
                if (section.childSections != null) {
                    for (SectionChild child : section.childSections) {
                        sectionIds.add(child.identityValue());
                    }
                }
            }
            return sectionIds;
        }

        private static List<WorkshopWidget> parseWidgets(IControlMsgHandler msgHandler, Context context,
                                                         JSONArray widgetsJson,
                                                         Set<String> sectionIds,
                                                         boolean sortOrderFromEnvelope) {
            List<WorkshopWidget> widgets = new ArrayList<>();
            if (widgetsJson == null) {
                return widgets;
            }
            Set<String> names = new HashSet<>();
            for (int i = 0; i < widgetsJson.size(); i++) {
                JSONObject itemJson = widgetsJson.getJSONObject(i);
                WorkshopWidget widget = parseEntity(msgHandler, context,
                        itemJson, "widget[" + i + "]", WorkshopWidget.class);
                if (sortOrderFromEnvelope) {
                    widget.sortOrder = readEnvelopeSortOrder(itemJson,
                            "widget[" + widget.identityValue() + "]");
                }
                String sectionId = StringUtils.trimToNull(itemJson.getString(KEY_SECTION_ID));
                if (sectionId == null) {
                    throw new IllegalStateException("widget[" + widget.identityValue() + "] envelope '"
                            + KEY_SECTION_ID + "' can not be empty");
                }
                if (!sectionIds.contains(sectionId)) {
                    throw new IllegalStateException("widget[" + widget.identityValue()
                            + "] refers to unknown section: " + sectionId);
                }
                widget.sectionId = sectionId;
                if (!names.add(widget.identityValue())) {
                    throw new IllegalStateException("duplicate widget id in module: " + widget.identityValue());
                }
                widgets.add(widget);
            }
            return widgets;
        }

        /**
         * 用 POST 形态的 item JSON 构造实体实例。
         *
         * <p>解析、校验与实例化统一交给 {@link WorkshopFormParser}：必填、{@code Validator}、
         * 嵌套子表单（如 section 的 {@code childSections}）都由框架完成。
         * 改造前这里是 {@code createDescribable(...).getInstance()} —— 只解析不校验，
         * 非法枚举、缺字段会一路带进落盘流程。
         */
        private static <T> T parseEntity(IControlMsgHandler msgHandler, Context context,
                                         JSONObject itemJson, String label, Class<T> expectedType) {
            return WorkshopFormParser.parse(msgHandler, context, itemJson, label, expectedType);
        }

        /**
         * 解析页面数组。
         *
         * <p>线与 section / overlay / widget 完全一致：{@code {impl, vals}} 的 POST 表单项，
         * 由 {@link WorkshopFormParser} 校验并实例化。{@code template} 是
         * {@link com.qlangtech.tis.plugin.ontology.workshop.model.page.PageTemplate} 枚举，
         * 框架按常量名（{@code BLANK} / {@code TWO_COLUMN} …）反序列化，前端投影时负责大小写归一。
         *
         * <p>信封 {@code sortOrder} 不是表单字段语义（快照路径由数组顺序表达、delta 路径由客户端
         * 实体给出），因此实例化后再覆盖。
         *
         * @param normalizeByIndex {@code true} = 整对象快照路径，序号取数组下标；
         *                         {@code false} = delta 路径，序号取信封字段（必填）
         */
        private static List<WorkshopPage> parsePages(IControlMsgHandler msgHandler, Context context,
                                                     JSONArray pagesJson, boolean normalizeByIndex) {
            if (pagesJson == null) {
                throw new IllegalStateException("snapshot '" + KEY_PAGES + "' must be an array");
            }
            List<WorkshopPage> pages = new ArrayList<>(pagesJson.size());
            Set<String> ids = new HashSet<>();
            for (int i = 0; i < pagesJson.size(); i++) {
                JSONObject pageJson = pagesJson.getJSONObject(i);
                WorkshopPage page = parseEntity(msgHandler, context,
                        pageJson, "page[" + i + "]", WorkshopPage.class);
                // 框架校验保证 id 非空；唯一性不是单实例校验能覆盖的，仍在这里检查
                if (!ids.add(page.identityValue())) {
                    throw new IllegalStateException("duplicate page id: " + page.identityValue());
                }
                page.sortOrder = normalizeByIndex
                        // 整对象快照路径：序号由数组顺序表达
                        ? i
                        // delta 路径：序号是客户端实体上的权威值，服务端不再归一化
                        : readEnvelopeSortOrder(pageJson, "page[" + page.identityValue() + "]");
                pages.add(page);
            }
            return pages;
        }

        // ==================================================================
        //  WorkshopHeader（模块页头）
        //
        //  与 module 是 1:1，由 WorkshopHeaderStore 独立落盘
        //  （ontology/{domain}/workshop_headers/{moduleName}.xml）。
        //  前端配置入口在 Workshop 编辑器的 Layout 面板。
        // ==================================================================

        /**
         * header-get: 查询模块的页头配置（尚未配置过返回默认值，visible = true）
         */
        private void doHeaderGet(IControlMsgHandler msgHandler,
                                 IPluginContext pluginContext,
                                 Context context) throws Exception {
            String ontologyDomainId = msgHandler.getString("ontologyDomainId");
            String moduleName = msgHandler.getString("name");

            if (StringUtils.isEmpty(ontologyDomainId) || StringUtils.isEmpty(moduleName)) {
                throw new IllegalStateException("param 'ontologyDomainId' and 'name' can not be empty");
            }

            WorkshopHeader header = createModuleService().getHeader(ontologyDomainId, moduleName);

            JSONObject result = new JSONObject();
            result.put(IAjaxResult.KEY_SUCCESS, true);
            result.put("header", headerToJSON(header, moduleName));
            pluginContext.setBizResult(context, result);
        }

        /**
         * header-update: 覆盖式更新页头配置。
         *
         * <p>提交的是<b>整份</b> header 实例（{@code {impl, vals}} 形态），而不是字段 patch。
         * 这不是偏好问题：{@code orientation} / {@code collapseConfig} 是聚合属性字段，
         * 切换方向意味着换一个<b>子类</b>（丢掉 {@code HeightOrientation.height}、
         * 带上 {@code VerticalOrientation.width}），逐字段 patch 表达不了「换子类」这件事。
         *
         * <p>解析、校验与实例化统一交给 {@link WorkshopFormParser}（{@code AttrValMap} +
         * {@code validateWithScope(STRICT)} + {@code newInstance()}），与页面 / 分区 / widget
         * 保存链路同一套写法。改造前这里是手写的 11 个字段映射 + {@code String(v)} 参数拼接，
         * 那套方式天生承载不了嵌套结构。
         *
         * <p>{@code id} 以 URL 参数为准，避免表单里的值被改动后写到别的模块文件上。
         *
         * <p>header item 作为 <b>JSON body</b> 提交（路由参数仍在 query string）——
         * {@code AttrValMap.parseDescribableMap} 需要的是反序列化后的对象，而 body 由
         * {@code getJSONPostContent()} 统一解析。
         */
        private void doHeaderUpdate(IControlMsgHandler msgHandler,
                                    IPluginContext pluginContext,
                                    Context context) throws Exception {
            String ontologyDomainId = msgHandler.getString("ontologyDomainId");
            String moduleName = msgHandler.getString("name");

            if (StringUtils.isEmpty(ontologyDomainId) || StringUtils.isEmpty(moduleName)) {
                throw new IllegalStateException("param 'ontologyDomainId' and 'name' can not be empty");
            }

            JSONObject headerJson = pluginContext.getJSONPostContent();
            if (headerJson == null) {
                throw new IllegalStateException("json body (header item) can not be empty");
            }
            WorkshopHeader header = WorkshopFormParser.parse(msgHandler, context,
                    headerJson, "header", WorkshopHeader.class);
            // id 是落盘文件名，以 URL 参数为准
            header.id = moduleName;

            WorkshopModuleService svc = createModuleService();
            WorkshopHeader saved = svc.saveHeader(pluginContext, Optional.of(context),
                    ontologyDomainId, moduleName, header);

            JSONObject result = new JSONObject();
            result.put(IAjaxResult.KEY_SUCCESS, true);
            result.put("header", headerToJSON(saved, moduleName));
            pluginContext.setBizResult(context, result);
        }

        private static JSONObject headerToJSON(WorkshopHeader header, String moduleName) throws Exception {
            JSONObject json = WorkshopHeader.toJSON(header);
            json.put("moduleId", moduleName);
            return json;
        }

        private static JSONObject pageToJSON(WorkshopPage page) {
            JSONObject pj = new JSONObject();
            // id 是页面主键（UUID，identity）也是引用/寻址键；name 是可编辑的显示名
            pj.put("id", page.identityValue());
            pj.put("name", page.name);
            pj.put("displayName", page.displayName);
            pj.put("template", page.template != null ? page.template.name().toLowerCase() : "blank");
            pj.put("sortOrder", page.sortOrder != null ? page.sortOrder : 0);
            return pj;
        }

        // ==================================================================
        //  JSON 序列化辅助
        // ==================================================================

        private static JSONObject toResultJSON(WorkshopModule module) {
            JSONObject json = new JSONObject();
            json.put("id", module.getId());
            json.put("name", module.name);
            json.put("description", module.description);
            // json.put("ontologyDomainId", module.ontologyDomainId);
            json.put("stateSavingEnabled", module.stateSavingEnabled);
            json.put("createdBy", module.getCreatedBy());
            json.put("createdAt", module.getCreatedAt() != null ? module.getCreatedAt().toString() : null);
            json.put("updatedBy", module.getUpdatedBy());
            json.put("updatedAt", module.getUpdatedAt() != null ? module.getUpdatedAt().toString() : null);

            if (module.pages != null) {
                json.put("pagesCount", module.pages.size());
            }
            if (module.variables != null) {
                json.put("variablesCount", module.variables.size());
            }
            if (module.overlays != null) {
                json.put("overlaysCount", module.overlays.size());
            }
            return json;
        }
    }
}
