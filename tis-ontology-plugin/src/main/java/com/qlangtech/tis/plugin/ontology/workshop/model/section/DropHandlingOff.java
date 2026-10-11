package com.qlangtech.tis.plugin.ontology.workshop.model.section;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/10
 */
public class DropHandlingOff extends DropHandling {
    @TISExtension
    public static class DefaultDescriptor extends Descriptor<DropHandling> {
        @Override
        public String getDisplayName() {
            return SWITCH_OFF;
        }
    }
}
