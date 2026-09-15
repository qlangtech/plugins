package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;

import java.io.Serializable;
import java.time.Duration;

/**
 * 自动刷新配置
 */
public class AutoRefreshConfig implements Describable<AutoRefreshConfig>, Serializable {

    private static final long serialVersionUID = 1L;

    @FormField(ordinal = 0, type = FormFieldType.ENUM)
    public Boolean enabled = false;

    @FormField(ordinal = 1, type = FormFieldType.DURATION_OF_SECOND)
    public Duration intervalSeconds;

    @FormField(ordinal = 2, type = FormFieldType.ENUM)
    public Boolean allowUserControl = true;

    @TISExtension
    public static class DefaultDescriptor extends Descriptor<AutoRefreshConfig> {
        @Override
        public String getDisplayName() {
            return "Auto Refresh Config";
        }
    }
}
