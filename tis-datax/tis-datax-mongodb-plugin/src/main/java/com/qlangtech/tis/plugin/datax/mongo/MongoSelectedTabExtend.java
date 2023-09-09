package com.qlangtech.tis.plugin.datax.mongo;

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.SelectedTabExtend;
import com.qlangtech.tis.plugin.datax.mongo.reader.ReaderFilter;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/9/1
 */
public class MongoSelectedTabExtend extends SelectedTabExtend {

    @FormField(ordinal = 1, validate = {Validator.require})
    public ReaderFilter filter;

    @Override
    public ExtendType getExtendType() {
        return ExtendType.BATCH_SOURCE;
    }

    @TISExtension
    public static class DefaultExtendTabDescriptor extends BaseDescriptor {

    }

}
