package com.qlangtech.tis.plugin.ontology.workshop;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.datax.IManipulateStatus;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorUseableShortComment;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.lang.PayloadLink;
import com.qlangtech.tis.manage.common.IAjaxResult;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.IdentityDesc;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.KeyedPluginStore;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.manipulate.ManipulatePluginCacheRegister;
import com.qlangtech.tis.plugin.ontology.OntologyDomain;
import com.qlangtech.tis.plugin.ontology.OntologyDomainManipulate;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.plugin.ontology.workshop.model.AutoRefreshConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.RoutingConfig;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopHeader;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopOverlay;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopPage;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopVariable;
import com.qlangtech.tis.plugin.ontology.workshop.service.WorkshopModuleService;
import com.qlangtech.tis.plugin.ontology.workshop.store.WorkshopVariableStore;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.io.FileUtils;
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
    /**
     * <pre>
     *     "pages": {
     *     "label": "页面列表",
     *     "help": "模块包含的所有页面"
     *   },
     *   "overlays": {
     *     "label": "浮层列表",
     *     "help": "模块包含的所有浮层（抽屉、模态框）"
     *   },
     *   "variables": {
     *     "label": "变量列表",
     *     "help": "模块级别的全局变量定义"
     *   },
     *   "header": {
     *     "label": "页面头部",
     *     "help": "模块的页面头部配置"
     *   }
     *
     * </pre>
     */
    // 聚合子实体（使用强类型 List）
    // @FormField(ordinal = 10, type = FormFieldType.MULTI_SELECTABLE)
    public transient List<WorkshopPage> pages = new ArrayList<>();

    // @FormField(ordinal = 11, type = FormFieldType.MULTI_SELECTABLE)
    public transient List<WorkshopOverlay> overlays = new ArrayList<>();

    // @FormField(ordinal = 12, type = FormFieldType.MULTI_SELECTABLE)
    public transient List<WorkshopVariable> variables = new ArrayList<>();

    // @FormField(ordinal = 13)
    public transient WorkshopHeader header;

    // 非表单字段（不需要 @FormField）
    private String id;
    private String createdBy;
    private LocalDateTime createdAt;
    private String updatedBy;
    private LocalDateTime updatedAt;

//    // 构造函数
//    public WorkshopModule() {
//
//    }

    public static WorkshopModule load(String ontologyDomain, String modeuleName) {
        ManipulatePluginCacheRegister.TemplateManipulateStore<OntologyDomainManipulate> manipulateStore
                = getManipulateStore(ontologyDomain, false);
        WorkshopModule module = manipulateStore.getManipuldate(IdentityName.create(modeuleName), WorkshopModule.class);
        loadVariables(ontologyDomain, modeuleName, module);
        return module;
    }

    public static List<WorkshopModule> loadAll(String ontologyDomain) {
        ManipulatePluginCacheRegister.TemplateManipulateStore<OntologyDomainManipulate> manipulateStore
                = getManipulateStore(ontologyDomain, false);
        List<WorkshopModule> modules = manipulateStore.getManipuldaties(WorkshopModule.class);
        for (WorkshopModule module : modules) {
            loadVariables(ontologyDomain, module.name, module);
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
        module.variables = WorkshopVariableStore.create(ontologyDomain, moduleName).listAll();
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

    public WorkshopPage findPageById(String pageId) {
        if (pages == null) return null;
        return pages.stream()
                .filter(p -> p.getId().equals(pageId))
                .findFirst()
                .orElse(null);
    }

    public WorkshopVariable findVariableByName(String variableName) {
        if (variables == null) return null;
        return variables.stream()
                .filter(v -> v.getName().equalsIgnoreCase(variableName))
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
                    default:
                        pluginContext.addErrorMessage(context, "Unsupported workshop module operation type: " + type);
                }
            } catch (Exception e) {
                pluginContext.addErrorMessage(context, "操作失败: " + e.getMessage());
            }
        }

        public  static final WorkshopModuleService workshopModuleSvc = new WorkshopModuleService();

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
            List<WorkshopPage> pages = listAllPages(ontologyDomainId);
            if (!pages.isEmpty()) {
                JSONArray pagesJson = new JSONArray();
                for (WorkshopPage p : pages) {
                    JSONObject pj = new JSONObject();
                    pj.put("id", p.id);
                    pj.put("name", p.name);
                    pj.put("displayName", p.displayName);
                    pj.put("template", p.template != null ? p.template.name().toLowerCase() : "blank");
                    pj.put("sortOrder", p.sortOrder != null ? p.sortOrder : 0);
                    pj.put("moduleId", module.getId());
                    pagesJson.add(pj);
                }
                moduleJson.put("pages", pagesJson);
            }

            // 变量同样来自独立存储（variables 是 transient），且下发 {impl, vals} 形态
            // 以便前端回填插件表单，因此不能像 pages 那样手写 JSON
            List<WorkshopVariable> variables = module.variables == null
                    ? Collections.emptyList() : module.variables;
            if (!variables.isEmpty()) {
                JSONArray variablesJson = new JSONArray();
                for (WorkshopVariable v : variables) {
                    JSONObject vj = WorkshopVariable.DefaultDescriptor.toVariableJSON(v);
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
        //  存储路径: TIS.pluginCfgRoot/ontology/{domain}/workshop_pages/{pageId}.xml
        // ==================================================================

        /**
         * 获取 WorkshopPage 的独立 PluginStore
         */
        private IPluginStore<WorkshopPage> getPagePluginStore(String domain, String pageId) {
            KeyedPluginStore.Key<WorkshopPage> key = new KeyedPluginStore.Key<WorkshopPage>(
                    OntologyDomain.ONTOLOGY_DOMAIN.getIdentity(),
                    domain + "/workshop_pages",
                    WorkshopPage.class) {
                @Override
                public String getSerializeFileRelativePath() {
                    return this.getSubDirPath() + File.separator + pageId;
                }
            };
            return TIS.getPluginStore(key);
        }

        /**
         * 获取 WorkshopPage 存储目录
         */
        private File getPageStoreDir(String domain) {
            KeyedPluginStore.Key<WorkshopPage> key = new KeyedPluginStore.Key<WorkshopPage>(
                    OntologyDomain.ONTOLOGY_DOMAIN.getIdentity(),
                    domain + "/workshop_pages",
                    WorkshopPage.class);
            return new File(TIS.pluginCfgRoot, key.getSubDirPath());
        }

        /**
         * 加载单个 Page
         */
        @SuppressWarnings("all")
        private WorkshopPage loadPage(String domain, String pageId) {
            if (StringUtils.isEmpty(pageId)) {
                return null;
            }
            IPluginStore<WorkshopPage> store = getPagePluginStore(domain, pageId);
            return store.getPlugin();
        }

        /**
         * 列出 domain 下的所有 Page
         */
        private List<WorkshopPage> listAllPages(String domain) {
            File pagesDir = getPageStoreDir(domain);
            if (!pagesDir.exists()) {
                return Collections.emptyList();
            }
            List<WorkshopPage> pages = new ArrayList<>();
            for (File f : FileUtils.listFiles(pagesDir, new String[]{"xml"}, false)) {
                String pageId = StringUtils.removeEnd(f.getName(), ".xml");
                WorkshopPage page = loadPage(domain, pageId);
                if (page != null) {
                    pages.add(page);
                }
            }
            // 按 sortOrder 排序
            pages.sort((a, b) -> {
                int ao = a.sortOrder != null ? a.sortOrder : 0;
                int bo = b.sortOrder != null ? b.sortOrder : 0;
                return Integer.compare(ao, bo);
            });
            return pages;
        }

        /**
         * page-get: 查询单个 Page
         */
        private void doPageGet(IControlMsgHandler msgHandler,
                               IPluginContext pluginContext,
                               Context context) {
            String ontologyDomainId = msgHandler.getString("ontologyDomainId");
            String pageId = msgHandler.getString("pageId");

            if (StringUtils.isEmpty(ontologyDomainId) || StringUtils.isEmpty(pageId)) {
                throw new IllegalStateException("param 'ontologyDomainId' and 'pageId' can not be empty");
            }

            WorkshopPage page = loadPage(ontologyDomainId, pageId);
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
         * page-list: 列出 domain 下所有 Page
         */
        private void doPageList(IControlMsgHandler msgHandler,
                                IPluginContext pluginContext,
                                Context context) {
            String ontologyDomainId = msgHandler.getString("ontologyDomainId");

            if (StringUtils.isEmpty(ontologyDomainId)) {
                throw new IllegalStateException("param 'ontologyDomainId' can not be empty");
            }

            List<WorkshopPage> pages = listAllPages(ontologyDomainId);

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
            String pageId = msgHandler.getString("pageId");

            if (StringUtils.isEmpty(ontologyDomainId) || StringUtils.isEmpty(pageId)) {
                throw new IllegalStateException("param 'ontologyDomainId' and 'pageId' can not be empty");
            }

            WorkshopPage page = loadPage(ontologyDomainId, pageId);
            if (page == null) {
                pluginContext.addErrorMessage(context, "Page not found: " + pageId);
                return;
            }

            // Apply patch fields
            String name = msgHandler.getString("name");
            String displayName = msgHandler.getString("displayName");
            if (StringUtils.isNotEmpty(name)) {
                page.name = name;
            }
            if (displayName != null) {
                page.displayName = displayName;
            }

            IPluginStore<WorkshopPage> store = getPagePluginStore(ontologyDomainId, pageId);
            store.setPlugins(pluginContext, Optional.ofNullable(context),
                    Collections.singletonList(new Descriptor.ParseDescribable<>(page)), true);

            JSONObject result = new JSONObject();
            result.put(IAjaxResult.KEY_SUCCESS, true);
            result.put("page", pageToJSON(page));
            pluginContext.setBizResult(context, result);
        }

        private static JSONObject pageToJSON(WorkshopPage page) {
            JSONObject pj = new JSONObject();
            pj.put("id", page.id);
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
