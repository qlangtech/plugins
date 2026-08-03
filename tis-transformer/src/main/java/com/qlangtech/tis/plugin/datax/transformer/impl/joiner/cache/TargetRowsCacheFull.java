package com.qlangtech.tis.plugin.datax.transformer.impl.joiner.cache;

import com.alibaba.citrus.turbine.Context;
import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.transformer.UDFDesc;
import com.qlangtech.tis.plugin.datax.transformer.impl.joiner.TargetRowsCache;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.util.IPluginContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * 全量预加载维度表到内存的缓存策略。
 * <p>
 * 适用于维度表不大、且希望消除 N+1 点查开销的场景。
 * </p>
 *
 * @author 百岁 (baisui@qlangtech.com)
 */
public class TargetRowsCacheFull extends TargetRowsCache implements IPluginStore.AfterPluginSaved {

    @FormField(ordinal = 0, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Long maxRows = 200000L;

    private transient Map<JoinCacheKey, JoinCacheValue> _fullCache;
    private transient volatile boolean loaded;
    private transient final Object loadLock = new Object();

    @Override
    public boolean isOn() {
        return true;
    }

    @Override
    public boolean isFullPreload() {
        return true;
    }

    @Override
    public void preload(BulkLoader loader) {
        if (loaded) {
            return;
        }
        synchronized (loadLock) {
            if (loaded) {
                return;
            }
            Map<JoinCacheKey, JoinCacheValue> map = new HashMap<>();
            try {
                loader.load((key, val) -> {
                    if (map.size() >= maxRows) {
                        throw new IllegalStateException(
                                "dimension table row count exceeds maxRows=" + maxRows
                                        + ", please switch to LRU cache mode for large dimension tables");
                    }
                    map.putIfAbsent(key, val);
                });
            } catch (RuntimeException e) {
                throw e;
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            this._fullCache = map;
            this.loaded = true;
        }
    }

    @Override
    public JoinCacheValue getFromCache(JoinCacheKey key) {
        if (!loaded) {
            throw new IllegalStateException("full cache has not been preloaded");
        }
        return _fullCache.get(key);
    }

    @Override
    public JoinCacheValue set2Cache(JoinCacheKey key, JoinCacheValue val) {
        throw new UnsupportedOperationException("full-preload mode does not cache single rows");
    }

    @Override
    public List<UDFDesc> getUDFDesc() {
        return Collections.singletonList(new UDFDesc("maxRows", String.valueOf(maxRows)));
    }

    @Override
    public void afterSaved(IPluginContext pluginContext, Optional<Context> context) {
        this._fullCache = null;
        this.loaded = false;
    }

    @TISExtension
    public static class FullDesc extends Descriptor<TargetRowsCache> {

        public FullDesc() {
            super();
        }

        public boolean validateMaxRows(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            long n;
            try {
                n = Long.parseLong(value);
            } catch (NumberFormatException e) {
                msgHandler.addFieldError(context, fieldName, "必须为整数");
                return false;
            }
            if (n < 1 || n > 2_000_000L) {
                msgHandler.addFieldError(context, fieldName, "必须在 1 ~ 2,000,000 之间");
                return false;
            }
            return true;
        }

        @Override
        public String getDisplayName() {
            return "On(Full Preload)";
        }
    }
}
