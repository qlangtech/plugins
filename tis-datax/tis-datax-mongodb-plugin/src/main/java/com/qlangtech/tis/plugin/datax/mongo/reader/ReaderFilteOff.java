package com.qlangtech.tis.plugin.datax.mongo.reader;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/9/8
 */
public class ReaderFilteOff extends ReaderFilter {

    @TISExtension
    public static class DftDesc extends Descriptor<ReaderFilter> {
        @Override
        public String getDisplayName() {
            return SWITCH_OFF;
        }
    }
}
