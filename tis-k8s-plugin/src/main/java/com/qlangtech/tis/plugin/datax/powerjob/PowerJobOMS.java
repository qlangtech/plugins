package com.qlangtech.tis.plugin.datax.powerjob;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/10/31
 */
public class PowerJobOMS implements Describable<PowerJobOMS> {

    @FormField(ordinal = 0, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer akkaPort;
    @FormField(ordinal = 2, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer httpPort;

    @FormField(ordinal = 4, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer retentionLocal;
    @FormField(ordinal = 6, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer retentionRemote;


    @TISExtension()
    public static class DescriptorImpl extends Descriptor<PowerJobOMS> {
        public DescriptorImpl() {
            super();
        }

        @Override
        public String getDisplayName() {
            return "OMSProfile";
        }
    }

}
