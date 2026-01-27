package com.qlangtech.tis.plugin.datax.transformer.impl.joiner.cache;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.datax.transformer.UDFDesc;
import com.qlangtech.tis.plugin.datax.transformer.impl.joiner.TargetRowsCache;

import java.util.Collections;
import java.util.List;

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

    @Override
    public List<UDFDesc> getUDFDesc() {
        return Collections.singletonList(new UDFDesc("Enable", "false"));
    }

    @Override
    public JoinCacheValue getFromCache(JoinCacheKey key) {
        throw new UnsupportedOperationException();
    }

    @Override
    public JoinCacheValue set2Cache(JoinCacheKey key, JoinCacheValue val) {
        throw new UnsupportedOperationException();
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
