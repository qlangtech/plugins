package com.qlangtech.tis.plugin.datax.transformer.impl.joiner.cache;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.datax.transformer.impl.joiner.TargetRowsCache;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/1/15
 */
public class TargetRowsCacheOff extends TargetRowsCache {
    @Override
    public boolean isOn() {
        return false;
    }

    @TISExtension
    public static class OffDesc extends Descriptor<TargetRowsCache> {
        public OffDesc() {
            super();
        }

        @Override
        public String getDisplayName() {
            return SWITCH_OFF;
        }
    }
}
