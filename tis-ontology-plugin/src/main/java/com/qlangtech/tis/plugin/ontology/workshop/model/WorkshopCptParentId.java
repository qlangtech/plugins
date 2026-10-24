package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.plugin.ontology.workshop.enums.SectionParentType;

import java.io.Serializable;
import java.util.Objects;

/**
 * 父容器引用（类型 + 主键）—— {@code AbstractWorkshopSection.parent} 的类型。
 * <p>
 * 两个字段都是 {@code final}：归属关系只能整体替换，不能只改一半
 * （例如把 id 改成另一页却忘了把类型从 OVERLAY 改成 PAGE）。
 * 构造请走静态工厂 {@link #pageParent} / {@link #overlayParent} / {@link #sectionParent}。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/21
 */
public class WorkshopCptParentId implements Serializable {

    private static final long serialVersionUID = 1L;

    public final String parentId;
    /**
     * 父容器类型，与 {@link #parentId} 成对使用。取值与前端 {@code SectionBase.parentType}
     * 的字符串联合一致，见 {@link SectionParentType}。
     *
     * <p><b>不是表单字段</b>：同 {@link #parentId}，由服务端按「挂在谁下面」赋值。
     */
    public final SectionParentType parentType;

    public static WorkshopCptParentId pageParent(String parentId) {
        return new WorkshopCptParentId(parentId, SectionParentType.PAGE);
    }

    public static WorkshopCptParentId sectionParent(String parentId) {
        return new WorkshopCptParentId(parentId, SectionParentType.SECTION);
    }

    public static WorkshopCptParentId overlayParent(String parentId) {
        return new WorkshopCptParentId(parentId, SectionParentType.OVERLAY);
    }

    public WorkshopCptParentId(String parentId, SectionParentType parentType) {
        this.parentId = Objects.requireNonNull(parentId, "parentId can not be null");
        this.parentType = Objects.requireNonNull(parentType, "parentType can not be null");
    }
}
