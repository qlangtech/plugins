package com.qlangtech.tis.plugin.datax.mongo;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/8/30
 */
public class OffUpsertSupport extends UpsertSupport {
    @Override
    public boolean supportUpset() {
        return false;
    }

    @TISExtension
    public static class DftDesc extends Descriptor<UpsertSupport> {
        @Override
        public String getDisplayName() {
            return SWITCH_OFF;
        }
    }

}
