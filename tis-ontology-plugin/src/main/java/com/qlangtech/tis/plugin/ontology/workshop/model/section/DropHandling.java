package com.qlangtech.tis.plugin.ontology.workshop.model.section;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.ontology.workshop.model.EventConfig;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

/**
 * 拖放处理配置
 */
public class DropHandling implements Describable<DropHandling>, Serializable {

    @FormField(ordinal = 0, type = FormFieldType.ENUM)
    public Boolean enabled = false;

    @FormField(ordinal = 1, type = FormFieldType.MULTI_SELECTABLE)
    public List<EventConfig> onDropEvents = new ArrayList<>();

    @TISExtension
    public static class DefaultDescriptor extends Descriptor<DropHandling> {
        @Override
        public String getDisplayName() {
            return "Drop Handling";
        }
    }
}
