package com.qlangtech.tis.plugin.datax.mongo;

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.datax.SelectedTabExtend;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/9/1
 */
public class MongoSelectedTabExtend extends SelectedTabExtend {
    @Override
    public ExtendType getExtendType() {
        return ExtendType.BATCH_SOURCE;
    }

    @TISExtension
    public static class DefaultExtendTabDescriptor extends BaseDescriptor {

    }

}
