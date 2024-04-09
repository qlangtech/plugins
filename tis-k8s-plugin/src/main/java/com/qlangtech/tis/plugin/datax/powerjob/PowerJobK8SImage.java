package com.qlangtech.tis.plugin.datax.powerjob;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.config.k8s.impl.DefaultK8SImage;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.utils.TisMetaProps;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/12/17
 */
public class PowerJobK8SImage extends DefaultK8SImage {

    @FormField(ordinal = 5, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String // = "docker-registry.default.svc:5000/tis/tis-incr:latest";
            powerJobWorkerImagePath;
    @FormField(ordinal = 7, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String // = "docker-registry.default.svc:5000/tis/tis-incr:latest";
            embeddedMetaDataImagePath;

    public static final String dftPowerJobWorkerImagePath() {
        return "registry.cn-hangzhou.aliyuncs.com/tis/tis-datax-executor:"
                + TisMetaProps.getInstance().getVersion();
    }


    public static final String powerjobMetaStoreImagePath() {
        return "powerjob/powerjob-mysql:" + PowerJobCommonParams.getPowerJobVersion();
    }

    public static final String powerjobServerImagePath() {
        return "registry.cn-hangzhou.aliyuncs.com/tis/powerjob-server:" + PowerJobCommonParams.getPowerJobVersion();
    }


    @TISExtension()
    public static class DescriptorImpl extends DefaultK8SImage.DescriptorImpl {
        public DescriptorImpl() {
            super();
        }

        public final boolean validatePowerJobWorkerImagePath(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            return validateImagePath(msgHandler, context, fieldName, value);
        }

        public final boolean validateEmbeddedMetaDataImagePath(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            return validateImagePath(msgHandler, context, fieldName, value);
        }


        @Override
        protected ImageCategory getImageCategory() {
            return ImageCategory.DEFAULT_POWERJOB_DESC_NAME;
        }
    }
}
