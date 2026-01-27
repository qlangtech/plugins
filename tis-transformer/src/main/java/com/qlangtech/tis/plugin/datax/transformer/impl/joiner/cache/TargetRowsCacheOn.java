package com.qlangtech.tis.plugin.datax.transformer.impl.joiner.cache;

import com.alibaba.citrus.turbine.Context;
import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;
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
import com.qlangtech.tis.util.DurationFormat;
import com.qlangtech.tis.util.IPluginContext;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/1/15
 */
public class TargetRowsCacheOn extends TargetRowsCache implements IPluginStore.AfterPluginSaved {
    private transient Cache<JoinCacheKey, JoinCacheValue> _cache;

    @FormField(ordinal = 0, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Long maxSize;

    @FormField(ordinal = 1, type = FormFieldType.DURATION_OF_SECOND, validate = {Validator.require, Validator.integer})
    public Duration expireAfterWrite;

    @FormField(ordinal = 2, type = FormFieldType.DURATION_OF_SECOND, validate = {Validator.require, Validator.integer})
    public Duration expireAfterAccess;

    @Override
    public JoinCacheValue getFromCache(JoinCacheKey key) {
        return getCache().getIfPresent(key);
    }

    @Override
    public List<UDFDesc> getUDFDesc() {
        List<UDFDesc> descs = Lists.newArrayList();
        descs.add(new UDFDesc("maxSize", String.valueOf(this.maxSize)));
        descs.add(new UDFDesc("expireAfterWrite", DurationFormat.SHORT_EN.format(expireAfterWrite)));
        descs.add(new UDFDesc("expireAfterAccess", DurationFormat.SHORT_EN.format(expireAfterAccess)));
        return descs;
    }

    private Cache<JoinCacheKey, JoinCacheValue> getCache() {
        if (this._cache == null) {
            if (this.maxSize < 1) {
                throw new IllegalStateException("maxSize can not small than 1");
            }
            this._cache = Caffeine.newBuilder()
                    .maximumSize(this.maxSize)
                    .expireAfterWrite(expireAfterWrite)
                    .expireAfterAccess(expireAfterAccess)
                    .build();
        }
        return this._cache;
    }

    @Override
    public JoinCacheValue set2Cache(JoinCacheKey key, JoinCacheValue val) {
        getCache().put(key, Objects.requireNonNull(val, "val can not be null"));
        return val;
    }

    @Override
    public boolean isOn() {
        return true;
    }

    @Override
    public void afterSaved(IPluginContext pluginContext, Optional<Context> context) {
        this._cache = null;
    }

    @TISExtension
    public static class OnDesc extends Descriptor<TargetRowsCache> {
        static final Integer maxSize10w = 100000;
        static final Integer maxDuration10min = 600;

        public OnDesc() {
            super();
        }

        public boolean validateMaxSize(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            Integer maxSize = Integer.parseInt(value);
            if (maxSize < 1) {
                msgHandler.addFieldError(context, fieldName, "必须大于0");
                return false;
            }

            if (maxSize > maxSize10w) {
                msgHandler.addFieldError(context, fieldName, "不能超过" + maxSize10w);
                return false;
            }
            return true;
        }

        public boolean validateExpireAfterWrite(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            int duration = Integer.parseInt(value);
            if (duration < 1) {
                msgHandler.addFieldError(context, fieldName, "必须大于0");
                return false;
            }

            if (duration > maxDuration10min) {
                msgHandler.addFieldError(context, fieldName, "不能超过" + maxDuration10min);
                return false;
            }
            return true;
        }

        public boolean validateExpireAfterAccess(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            return validateExpireAfterWrite(msgHandler, context, fieldName, value);
        }

        @Override
        public String getDisplayName() {
            return SWITCH_ON;
        }
    }
}
