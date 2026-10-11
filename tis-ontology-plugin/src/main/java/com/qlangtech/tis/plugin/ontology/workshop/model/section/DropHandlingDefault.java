package com.qlangtech.tis.plugin.ontology.workshop.model.section;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.ontology.workshop.model.event.EventConfig;

import java.util.ArrayList;
import java.util.List;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/10
 */
public class DropHandlingDefault extends DropHandling {
    @FormField(ordinal = 1, desClazz = EventConfig.class, type = FormFieldType.MULTI_DESCRIBLE_PLUGIN)
    public List<EventConfig> onDropEvents = new ArrayList<>();

    @TISExtension
    public static class DefaultDescriptor extends Descriptor<DropHandling> {
        @Override
        public String getDisplayName() {
            return SWITCH_ON;
        }
    }
}
