package com.qlangtech.tis.plugin.ontology.workshop.service;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.plugin.ontology.workshop.WorkshopModule;
import com.qlangtech.tis.plugin.ontology.workshop.exception.VariableAlreadyExistsException;
import com.qlangtech.tis.plugin.ontology.workshop.exception.VariableNotFoundException;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopOverlay;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopPage;
import com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopVariable;
import com.qlangtech.tis.plugin.ontology.workshop.store.WorkshopPageStore;
import com.qlangtech.tis.plugin.ontology.workshop.store.WorkshopVariableStore;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.UUID;

/**
 * Workshop Module 业务逻辑服务
 */
public class WorkshopModuleService {

    private static final Logger logger = LoggerFactory.getLogger(WorkshopModuleService.class);

//    /**
//     * 创建 Module
//     */
//    public WorkshopModule createModule(String name, String description, String ontologyDomainId, String createdBy) {
//        WorkshopModuleStore store = WorkshopStoreFactory.getStore(ontologyDomainId);
//
//        // 检查是否已存在
//        if (store.exists(name)) {
//            throw new ModuleAlreadyExistsException(
//                    String.format("Workshop Module '%s' already exists in domain '%s'", name, ontologyDomainId)
//            );
//        }
//
//        // 创建 Module
//        WorkshopModule module = new WorkshopModule();
//        module.name = name;
//        module.description = description;
//        // module.ontologyDomainId = ontologyDomainId;
//        module.setCreatedBy(createdBy);
//
//        // 保存
//        try {
//            store.save(module);
//            logger.info("Workshop Module '{}' created successfully", name);
//            return module;
//        } catch (IOException e) {
//            logger.error("Failed to create Workshop Module: " + name, e);
//            throw new ModuleSaveException("Failed to create Workshop Module", e);
//        }
//    }

    /**
     * 获取 Module
     */
    public WorkshopModule getModule(String ontologyDomainId, String moduleName) {
        return WorkshopModule.load(ontologyDomainId, moduleName);

//        WorkshopModuleStore store = WorkshopStoreFactory.getStore(ontologyDomainId);
//
//        try {
//            return store.load(moduleName);
//        } catch (IOException e) {
//            logger.error("Failed to load Workshop Module: " + moduleName, e);
//            throw new ModuleNotFoundException("Workshop Module not found: " + moduleName);
//        }
    }

    /**
     * 列出指定 Domain 下的所有 Module
     */
    public List<WorkshopModule> listModules(String ontologyDomainId) {
//        WorkshopModuleStore store = WorkshopStoreFactory.getStore(ontologyDomainId);
//        return store.listAll();
        return WorkshopModule.loadAll(ontologyDomainId);
    }

    /**
     * 更新 Module
     */
    public WorkshopModule updateModule(String ontologyDomainId, String moduleName, WorkshopModule updatedModule, String updatedBy) {
//        WorkshopModuleStore store = WorkshopStoreFactory.getStore(ontologyDomainId);
//
//        // 检查是否存在
//        if (!store.exists(moduleName)) {
//            throw new ModuleNotFoundException("Workshop Module not found: " + moduleName);
//        }
//
//        // 更新 updatedAt
//        updatedModule.setUpdatedAt(LocalDateTime.now());
//        updatedModule.setUpdatedBy(updatedBy);
//
//        // 保存
//        try {
//            store.save(updatedModule);
//            logger.info("Workshop Module '{}' updated successfully", moduleName);
//            return updatedModule;
//        } catch (IOException e) {
//            logger.error("Failed to update Workshop Module: " + moduleName, e);
//            throw new ModuleSaveException("Failed to update Workshop Module", e);
//        }
        throw new UnsupportedOperationException();
    }

    /**
     * 删除 Module
     */
    public void deleteModule(String ontologyDomainId, String moduleName) {
//        WorkshopModuleStore store = WorkshopStoreFactory.getStore(ontologyDomainId);
//
//        if (!store.exists(moduleName)) {
//            throw new ModuleNotFoundException("Workshop Module not found: " + moduleName);
//        }
//
//        boolean deleted = store.delete(moduleName);
//        if (!deleted) {
//            throw new ModuleSaveException("Failed to delete Workshop Module: " + moduleName);
//        }
//
//        logger.info("Workshop Module '{}' deleted successfully", moduleName);
    }

    /**
     * 重命名 Module
     */
    public void renameModule(String ontologyDomainId, String oldName, String newName) {
//        WorkshopModuleStore store = WorkshopStoreFactory.getStore(ontologyDomainId);
//
//        try {
//            store.rename(oldName, newName);
//            logger.info("Workshop Module renamed from '{}' to '{}'", oldName, newName);
//        } catch (IOException e) {
//            logger.error("Failed to rename Workshop Module", e);
//            throw new ModuleSaveException("Failed to rename Workshop Module", e);
//        }
        throw new UnsupportedOperationException();
    }

    /**
     * 添加 Page 到 Module
     */
    public WorkshopModule addPage(String ontologyDomainId, String moduleName, WorkshopPage page, String updatedBy) {
        WorkshopModule module = getModule(ontologyDomainId, moduleName);
        module.addPage(page);
        return updateModule(ontologyDomainId, moduleName, module, updatedBy);
    }

    // ==================================================================
    //  Page 存储
    //
    //  与变量同理：页面也不走模块 XML（WorkshopPage.sections 是 transient），
    //  由 WorkshopPageStore 独立落盘（ontology/{domain}/workshop_pages/{pageId}.xml）。
    //  页面按 domain 归属（不带 moduleName 维度），故这里只需 domain。
    // ==================================================================

    /**
     * 取得某个 domain 的页面存储
     */
    public WorkshopPageStore getPageStore(String ontologyDomainId) {
        return WorkshopPageStore.create(ontologyDomainId);
    }

    /**
     * 列出 domain 下的所有页面，按 sortOrder 升序
     */
    public List<WorkshopPage> listPages(String ontologyDomainId) {
        return getPageStore(ontologyDomainId).listAll();
    }

    // ==================================================================
    //  变量 CRUD
    //
    //  变量不走模块 XML：WorkshopModule.variables 是 transient 字段，
    //  XStream 会跳过它，所以变量由 WorkshopVariableStore 独立落盘
    //  （ontology/{domain}/workshop_variables/{moduleName}/{variableId}.xml）。
    //  也因此原先「取模块 → 往 module.variables 里加 → 回写模块」的写法在此不适用：
    //  updateModule() 本就是未实现的 UnsupportedOperationException。
    // ==================================================================

    /**
     * 取得某个模块的变量存储
     */
    public WorkshopVariableStore getVariableStore(String ontologyDomainId, String moduleName) {
        return WorkshopVariableStore.create(ontologyDomainId, moduleName);
    }

    /**
     * 列出模块下的所有变量
     */
    public List<WorkshopVariable> listVariables(String ontologyDomainId, String moduleName) {
        return getVariableStore(ontologyDomainId, moduleName).listAll();
    }

    /**
     * 查询单个变量，不存在抛 {@link VariableNotFoundException}
     */
    public WorkshopVariable getVariable(String ontologyDomainId, String moduleName, String variableId) {
        if (StringUtils.isEmpty(variableId)) {
            throw new IllegalArgumentException("param variableId can not be empty");
        }
        WorkshopVariable variable = getVariableStore(ontologyDomainId, moduleName).load(variableId);
        if (variable == null) {
            throw new VariableNotFoundException("Variable not found: " + variableId
                    + " in module '" + moduleName + "'");
        }
        return variable;
    }

    /**
     * 新建变量。模块内变量名唯一（大小写不敏感）。
     */
    public WorkshopVariable addVariable(IPluginContext pluginContext, Optional<Context> context,
                                        String ontologyDomainId, String moduleName, WorkshopVariable variable) {
        Objects.requireNonNull(variable, "param variable can not be null");
        WorkshopVariableStore store = getVariableStore(ontologyDomainId, moduleName);

        if (StringUtils.isEmpty(variable.getId())) {
            variable.setId(UUID.randomUUID().toString());
        }
        if (store.findByName(variable.getName()) != null) {
            throw new VariableAlreadyExistsException(
                    String.format("Variable '%s' already exists in module '%s'", variable.getName(), moduleName));
        }

        store.save(pluginContext, context, variable, false);
        logger.info("Workshop Variable '{}' created in module '{}'", variable.getName(), moduleName);
        return variable;
    }

    /**
     * 更新变量。唯一性校验会排除变量自身，否则改名以外的任何保存都会被自己挡住。
     */
    public WorkshopVariable updateVariable(IPluginContext pluginContext, Optional<Context> context,
                                           String ontologyDomainId, String moduleName, WorkshopVariable variable) {
        Objects.requireNonNull(variable, "param variable can not be null");
        if (StringUtils.isEmpty(variable.getId())) {
            throw new IllegalStateException("variable id can not be empty");
        }
        WorkshopVariableStore store = getVariableStore(ontologyDomainId, moduleName);
        if (store.load(variable.getId()) == null) {
            throw new VariableNotFoundException("Variable not found: " + variable.getId()
                    + " in module '" + moduleName + "'");
        }

        WorkshopVariable sameName = store.findByName(variable.getName());
        if (sameName != null && !variable.getId().equals(sameName.getId())) {
            throw new VariableAlreadyExistsException(
                    String.format("Variable '%s' already exists in module '%s'", variable.getName(), moduleName));
        }

        store.save(pluginContext, context, variable, true);
        logger.info("Workshop Variable '{}' updated in module '{}'", variable.getName(), moduleName);
        return variable;
    }

    /**
     * 删除变量
     */
    public void deleteVariable(IPluginContext pluginContext, Optional<Context> context,
                               String ontologyDomainId, String moduleName, String variableId) {
        if (StringUtils.isEmpty(variableId)) {
            throw new IllegalArgumentException("param variableId can not be empty");
        }
        WorkshopVariableStore store = getVariableStore(ontologyDomainId, moduleName);
        if (!store.delete(pluginContext, context, variableId)) {
            throw new VariableNotFoundException("Variable not found: " + variableId
                    + " in module '" + moduleName + "'");
        }
        logger.info("Workshop Variable '{}' deleted from module '{}'", variableId, moduleName);
    }

    /**
     * 添加 Overlay 到 Module
     */
    public WorkshopModule addOverlay(String ontologyDomainId, String moduleName, WorkshopOverlay overlay, String updatedBy) {
        WorkshopModule module = getModule(ontologyDomainId, moduleName);
        module.addOverlay(overlay);
        return updateModule(ontologyDomainId, moduleName, module, updatedBy);
    }
}
