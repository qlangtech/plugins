package com.qlangtech.tis.plugin.ontology.workshop.enums;

/**
 * Section 的父容器类型 —— 与前端 {@code SectionBase.parentType} 的字符串联合
 * {@code 'page' | 'overlay' | 'section'} 逐字对应（{@code tis-console/src/workshop/models/section.model.ts}）。
 *
 * <p>作为 {@link com.qlangtech.tis.plugin.ontology.workshop.model.WorkshopCptParentId#parentType}
 * 与同一对象里的 {@code parentId} 成对出现，共同表达「子侧外键 + 父类型」的归属：
 * <ul>
 *   <li>{@link #PAGE} → {@code parentId} 是 {@code WorkshopPage.id}（UUID）</li>
 *   <li>{@link #OVERLAY} → {@code parentId} 是 {@code WorkshopOverlay.id}（UUID）</li>
 *   <li>{@link #SECTION} → {@code parentId} 是父 {@code AbstractWorkshopSection.id}（UUID）</li>
 * </ul>
 *
 * <p>本枚举<b>不是</b>表单字段（section 的归属由服务端按请求参数赋值），所以不需要
 * {@code DescriptorUseableShortComment}；但协议上的取值必须与前端字面量一致，
 * 因此每个常量带一个 {@link #getLiteral()}，解析一律走 {@link #parse(String)}，
 * 不要用 {@code valueOf(upperCase(...))} 散落各处 —— 取值对不上时要立刻报错，
 * 不能让 section 静默挂到一个不存在的父类型下。
 *
 * <p>XStream 落盘时写的是枚举常量名（{@code PAGE} / {@code OVERLAY} / {@code SECTION}）。
 */
public enum SectionParentType {

    PAGE("page"),
    OVERLAY("overlay"),
    SECTION("section");

    /**
     * 与前端 {@code parentType} 联合类型逐字对应的协议值，改名会打断前后端约定
     */
    private final String literal;

    SectionParentType(String literal) {
        this.literal = literal;
    }

    public String getLiteral() {
        return this.literal;
    }

    /**
     * @throws IllegalArgumentException literal 不在 page / overlay / section 之内
     */
    public static SectionParentType parse(String literal) {
        for (SectionParentType type : values()) {
            if (type.literal.equals(literal)) {
                return type;
            }
        }
        throw new IllegalArgumentException("illegal section parentType:" + literal
                + ", expect one of page/overlay/section");
    }
}
