package com.qlangtech.tis.plugin.ontology.workshop.model.section;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;

import java.io.Serializable;

/**
 * Section 布局配置（抽象基类，支持多态）
 */
public abstract class SectionLayoutConfig implements Describable<SectionLayoutConfig>, Serializable {

    private static final long serialVersionUID = 1L;

    protected abstract static class BasicDescriptor extends Descriptor<SectionLayoutConfig> {
    }
}
