package com.qlangtech.tis.plugin.ontology.workshop;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.qlangtech.tis.datax.IManipulateStatus;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
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
import com.qlangtech.tis.plugin.ontology.impl.storegetter.BaiscAssistStoreGetter;
import com.qlangtech.tis.plugin.ontology.workshop.model.AutoRefreshConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.RoutingConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopHeader;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopOverlay;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopPage;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopVariable;
import com.qlangtech.tis.plugin.ontology.workshop.service.WorkshopModuleService;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.util.AttrValMap;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.qlangtech.tis.plugin.ontology.OntologyDomain.getOntologyDomainDir;

/**
 * Workshop Module 聚合根实体
 * <p>
 * 作为 TIS 插件实现，支持通过 UI 表单配置
 */
public class WorkshopModule extends OntologyDomainManipulate implements IManipulateStatus //
        , IdentityDesc<JSONObject>, IPluginStore.BeforePluginSaved, IPluginStore.AfterPluginSaved {

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

    public List<WorkshopPage> loadAllPage() {
        return Lists.newArrayList();
    }

    public static abstract class BasicWorkshopStoreGetter<T extends Describable<?>> extends BaiscAssistStoreGetter<T> {



        @Override
        public File getAssistRootDir(String ontologyName, Optional<String> submodule) {
            return getWorkshopDir(ontologyName, submodule.orElseThrow());
        }

        @Override
        public File getAssistRootDir(String ontologyName) {
            // return super.getAssistRootDir(ontologyName);
            throw new UnsupportedOperationException("ontologyName:" + ontologyName);
        }

//        /**
//         * 与父类实现逐行一致，唯一的差别是 Key 的 pluginClass 取自 {@link #getPluginType()}
//         * 而不是硬编码的 {@code ONTOLOGY.extensionPoint}。
//         * <p>
//         * 之所以整段就地覆盖而不是调用 super：Key 是父类在方法体内匿名构造的，
//         * 外部没有替换其 pluginClass 的机会，只能自己再构造一遍。
//         * 父类若改动落盘约定，这里需要同步跟进。
//         */
//        @SuppressWarnings("all")
//        @Override
//        public IPluginStore<T> getPluginStore(OntologyPluginMeta pluginMeta, Optional<String> submodule) {
//            if (pluginMeta.isUpdate() || pluginMeta.shallPersistence()) {
//                String ontologyName = pluginMeta.getDomain();
//                final String pluginIdVal = pluginMeta.getPluginIdVal();
//                if (StringUtils.isEmpty(ontologyName)) {
//                    throw new IllegalArgumentException("param ontologyName can not be empty");
//                }
//                if (StringUtils.isEmpty(pluginIdVal)) {
//                    throw new IllegalArgumentException("param pluginIdVal can not be empty");
//                }
//                KeyedPluginStore.Key key = new KeyedPluginStore.Key(Ontology.ONTOLOGY.getIdentity(), ontologyName
//                        , getPluginType()) {
//                    @Override
//                    public File getStoreFile() {
//                        File workshopDir = getAssistRootDir(ontologyName, submodule);
//                        return new File(workshopDir, Descriptor.getPluginFileName(getFileName()));
//                    }
//
//                    public String getSerializeFileRelativePath() {
//                        return this.getSubDirPath() + File.separator + getFileName();
//                    }
//
//                    @Override
//                    protected String getFileName() {
//                        return pluginIdVal;
//                    }
//
//                    @Override
//                    public int hashCode() {
//                        return getStoreFile().hashCode();
//                    }
//                };
//                return TIS.getPluginStore(key).unsaveCast();
//            }
//            return (IPluginStore<T>) IPluginStore.noSaveStore(pluginMeta.getDelegate());
//        }
    }


    public static WorkshopModule load(String ontologyDomain, String modeuleName) {
        ManipulatePluginCacheRegister.TemplateManipulateStore<OntologyDomainManipulate> manipulateStore
                = getManipulateStore(ontologyDomain, false);
        WorkshopModule module = manipulateStore.getManipuldate(IdentityName.create(modeuleName), WorkshopModule.class);
        loadVariables(ontologyDomain, modeuleName, module);
        loadHeader(ontologyDomain, modeuleName, module);
        return module;
    }

    public static List<WorkshopModule> loadAll(String ontologyDomain) {
        ManipulatePluginCacheRegister.TemplateManipulateStore<OntologyDomainManipulate> manipulateStore
                = getManipulateStore(ontologyDomain, false);
        List<WorkshopModule> modules = manipulateStore.getManipuldaties(WorkshopModule.class);
        for (WorkshopModule module : modules) {
            loadVariables(ontologyDomain, module.name, module);
            loadHeader(ontologyDomain, module.name, module);
        }
        return modules;
        //  return manipulateStore.getManipuldate(IdentityName.create(modeuleName), WorkshopModule.class);
    }

    /**
     * 从独立存储回填 {@link #variables}。
     * <p>
     * variables 是 transient 字段，模块 XML 里没有它（XStream 会跳过 transient），
     * 因此必须在这里补上，否则 {@link #findVariableByName(String)} 恒为 null，
     * WidgetOptionHelper 的变量下拉也拿不到数据。
     */
    private static void loadVariables(String ontologyDomain, String moduleName, WorkshopModule module) {
        if (module == null || StringUtils.isEmpty(moduleName)) {
            return;
        }
        //  module.variables = WorkshopVariableStore.create(ontologyDomain, moduleName).listAll();
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

    @Override
    public String identityValue() {
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
     * page-get / page-update 的寻址参数名。
     * <p>
     * 曾叫 {@code pageId}，传的是自动生成的 UUID；page 的 id 被删除、name 成为标识后，
     * 继续叫 id 会让人以为里面装的是 UUID，故改名。
     */
    public static final String PARAM_PAGE_NAME = "pageName";

    /**
     * 按 name 查页面 —— page 的 name 就是它的标识（曾叫 id，是自动生成的 UUID）。
     * 比较前先归一化，与 {@link WorkshopVariable#normalizeName} 的口径一致。
     */
    public WorkshopPage findPageByName(String pageName) {
        if (pages == null) return null;
        String normalized = WorkshopVariable.normalizeName(pageName);
        return pages.stream()
                .filter(p -> WorkshopVariable.normalizeName(p.name).equals(normalized))
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
     * 支持的 type 值：create | get | list | update | delete
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
                    case "page-update":
                        doPageUpdate(msgHandler, pluginContext, context);
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
                pluginContext.addErrorMessage(context, "操作失败: " + e.getMessage());
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
            List<WorkshopPage> pages = module.listAllPage();
            if (!pages.isEmpty()) {
                JSONArray pagesJson = new JSONArray();
                for (WorkshopPage p : pages) {
                    JSONObject pj = new JSONObject();
                    pj.put("name", p.name);
                    pj.put("displayName", p.displayName);
                    pj.put("template", p.template != null ? p.template.name().toLowerCase() : "blank");
                    pj.put("sortOrder", p.sortOrder != null ? p.sortOrder : 0);
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

            result.put("module", moduleJson);
            pluginContext.setBizResult(context, result);
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
        //  TIS.pluginCfgRoot/ontology/{domain}/workshop_pages/{pageId}.xml）。
        //  此处原先自建 PluginStore 的写法有一个缓存碰撞缺陷：pageId 只编进文件路径
        //  而没编进 Key.keyVal，导致同 domain 下所有页面共用一个缓存 store、
        //  listAll() 会把同一个页面重复返回 N 次。详见 WorkshopPageStore 类注释。
        // ==================================================================

        /**
         * page-get: 查询单个 Page
         */
        private void doPageGet(IControlMsgHandler msgHandler,
                               IPluginContext pluginContext,
                               Context context) {
            String ontologyDomainId = msgHandler.getString("ontologyDomainId");
            String pageName = msgHandler.getString(PARAM_PAGE_NAME);

            if (StringUtils.isEmpty(ontologyDomainId) || StringUtils.isEmpty(pageName)) {
                throw new IllegalStateException("param 'ontologyDomainId' and '" + PARAM_PAGE_NAME + "' can not be empty");
            }
            String moduleName = null;
            WorkshopPage page = WorkshopPage.loadPage(ontologyDomainId, moduleName, pageName);
            if (page == null) {
                pluginContext.addErrorMessage(context, "Page not found: " + pageName);
                return;
            }

            JSONObject result = new JSONObject();
            result.put(IAjaxResult.KEY_SUCCESS, true);
            result.put("page", pageToJSON(page));
            pluginContext.setBizResult(context, result);
        }

        /**
         * page-list: 列出 domain 下所有 Page
         */
        private void doPageList(IControlMsgHandler msgHandler,
                                IPluginContext pluginContext,
                                Context context) {
            String ontologyDomainId = msgHandler.getString("ontologyDomainId");

            if (StringUtils.isEmpty(ontologyDomainId)) {
                throw new IllegalStateException("param 'ontologyDomainId' can not be empty");
            }

            List<WorkshopPage> pages = createModuleService().listPages(ontologyDomainId);

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
         * page-update: 更新 Page 属性
         */
        private void doPageUpdate(IControlMsgHandler msgHandler,
                                  IPluginContext pluginContext,
                                  Context context) {
            String ontologyDomainId = msgHandler.getString("ontologyDomainId");
            String pageName = msgHandler.getString(PARAM_PAGE_NAME);

            if (StringUtils.isEmpty(ontologyDomainId) || StringUtils.isEmpty(pageName)) {
                throw new IllegalStateException("param 'ontologyDomainId' and '" + PARAM_PAGE_NAME + "' can not be empty");
            }
            String modeuleName = null;
          //  WorkshopPageStore pageStore = WorkshopPageStore.create(ontologyDomainId);
            WorkshopPage page = WorkshopPage.loadPage(ontologyDomainId,modeuleName,pageName);
            if (page == null) {
                pluginContext.addErrorMessage(context, "Page not found: " + pageName);
                return;
            }

            // 只允许改 displayName。name 是页面标识（落盘文件名 + 模块内一切引用键），
            // 改名会让 SwitchToPageConfig.pageName / WidgetTabConfig.targetPage 等引用全部悬空，
            // 因此**不接受**请求体里的 name —— 曾经这里有一句 `page.name = name`，已移除。
            String displayName = msgHandler.getString("displayName");
            if (displayName != null) {
                page.displayName = displayName;
            }

         //   pageStore.save(pluginContext, Optional.ofNullable(context), page, true);

            JSONObject result = new JSONObject();
            result.put(IAjaxResult.KEY_SUCCESS, true);
            result.put("page", pageToJSON(page));
            pluginContext.setBizResult(context, result);
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
         * <p>解析交给 {@code AttrValMap.parseDescribableMap()}，校验与嵌套子表单一并复用
         * 框架实现（同 {@code WorkshopVariableOperation#parseVariable}）。改造前这里是手写的
         * 11 个字段映射 + {@code String(v)} 参数拼接，那套方式天生承载不了嵌套结构。
         *
         * <p>{@code id} 以 URL 参数为准，避免表单里的值被改动后写到别的模块文件上。
         */
        private void doHeaderUpdate(IControlMsgHandler msgHandler,
                                    IPluginContext pluginContext,
                                    Context context) throws Exception {
            String ontologyDomainId = msgHandler.getString("ontologyDomainId");
            String moduleName = msgHandler.getString("name");

            if (StringUtils.isEmpty(ontologyDomainId) || StringUtils.isEmpty(moduleName)) {
                throw new IllegalStateException("param 'ontologyDomainId' and 'name' can not be empty");
            }

            String headerJson = msgHandler.getString(WorkshopHeader.PARAM_HEADER);
            if (StringUtils.isEmpty(headerJson)) {
                throw new IllegalStateException("param '" + WorkshopHeader.PARAM_HEADER + "' can not be empty");
            }
            AttrValMap attrValMap = AttrValMap.parseDescribableMap(Optional.empty(),
                    JSONObject.parseObject(headerJson));
            WorkshopHeader header = (WorkshopHeader) attrValMap.createDescribable(msgHandler, context).getInstance();
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
            // 不再下发 id：page 的标识就是 name，下发两个名字会让前端继续按 id 寻址
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

    private List<WorkshopPage> listAllPage() {
        return Lists.newArrayList();
    }
}
