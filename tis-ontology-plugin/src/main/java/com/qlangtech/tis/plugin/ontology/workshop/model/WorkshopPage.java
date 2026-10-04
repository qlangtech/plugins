package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ontology.workshop.model.page.PageTemplate;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * Workshop Page 实体
 * <p>
 * sections 使用 transient 修饰，不随 WorkshopPage 主 JSON 序列化。
 * <p>
 * 注意：Page 目前<b>没有活的服务端落盘路径</b>（{@code WorkshopPageStore} 整文件被注释、
 * {@link #loadPage} 恒返回 null），page 数据实际由前端 {@code ModuleStateService} 持有、
 * 并经 {@code page-update} 端点局部回写。{@code identityValue()} 为将来的独立落盘预留
 * 文件名语义。
 */
public class WorkshopPage implements Describable<WorkshopPage>, Serializable, IdentityName {

    private static final long serialVersionUID = 1L;

    /**
     * 页面标识，同时也是 identity 字段 —— 落盘文件名与模块内一切对 page 的引用
     * （{@code SwitchToPageConfig.pageName}、{@code WidgetTabConfig.targetPage}、
     * {@code PageRoutingConfig.targetPage}）用的都是它。
     * <p>
     * name 一经落盘即不可修改：改名会让上述引用全部悬空，也会移动落盘文件。
     */
    @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT,
            validate = {Validator.require, Validator.identity})
    public String name;

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT)
    public String displayName;

    @FormField(ordinal = 2, type = FormFieldType.ENUM, validate = {Validator.require})
    public PageTemplate template = PageTemplate.BLANK;

    @FormField(ordinal = 3, type = FormFieldType.INT_NUMBER)
    public Integer sortOrder = 0;

    // @FormField(ordinal = 10, type = FormFieldType.MULTI_SELECTABLE)
    public transient List<WorkshopSection> sections = new ArrayList<>();

    /**
     * @param pageName 页面标识，即 {@link #name}
     */
    public static WorkshopPage loadPage(String ontologyDomainId, String moduleName, String pageName) {
        return null;
    }

    @Override
    public String identityValue() {
        return this.name;
    }

    public void addSection(WorkshopSection section) {
        if (sections == null) {
            sections = new ArrayList<>();
        }
        sections.add(section);
    }

    @TISExtension
    public static class DefaultDescriptor extends Descriptor<WorkshopPage> {
        @Override
        public String getDisplayName() {
            return "Workshop Page";
        }
    }
}

