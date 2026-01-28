package com.qlangtech.tis.plugin.datax.transformer.impl.joiner;

import com.alibaba.fastjson.annotation.JSONField;
import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.plugin.datax.transformer.UDFDesc;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * 目标记录是否开启缓存
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/1/15
 * @see JoinerSetMatchConditionAndCols
 */
public abstract class TargetRowsCache implements Describable<TargetRowsCache>, Serializable {
    public abstract boolean isOn();

    public abstract List<UDFDesc> getUDFDesc();

    public static class JoinCacheValue extends HashMap<String, Object> {
        private boolean _null = true;

        public boolean isNull() {
            return _null;
        }

        public void setNull(boolean val) {
            this._null = val;
        }
    }

    public static class JoinCacheKey {
        private List<Object> params = Lists.newArrayList();
        private List<Object> primaryVals = Lists.newArrayList();

        public JoinCacheKey addParam(Object param) {
            this.params.add(param);
            return this;
        }

        public JoinCacheKey addPrimaryVal(Object param) {
            this.addParam(param);
            primaryVals.add(param);
            return this;
        }

        public int getPrimaryValsLength() {
            return this.primaryVals.size();
        }

        public Object getPrimaryVal(int index) {
            return this.primaryVals.get(index);
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof JoinCacheKey)) return false;
            JoinCacheKey cacheKey = (JoinCacheKey) o;
            if (params.size() != cacheKey.params.size()) return false;
            for (int i = 0; i < params.size(); i++) {
                if (!Objects.equals(params.get(i), cacheKey.params.get(i))) {
                    return false;
                }
            }
            return true;
            // return this.hashCode() == cacheKey.hashCode();
        }

        @Override
        public String toString() {
            return this.params.stream().map(String::valueOf).collect(Collectors.joining("_"));
        }

        @Override
        public int hashCode() {
            return Objects.hash(params.toArray());
        }
    }

    /**
     * 从缓存中加载历史信息
     *
     * @param key
     * @return
     */
    public abstract JoinCacheValue getFromCache(JoinCacheKey key);

    public abstract JoinCacheValue set2Cache(JoinCacheKey key, JoinCacheValue val);

    @JSONField(serialize = false)
    @Override
    public Descriptor<TargetRowsCache> getDescriptor() {
        return Describable.super.getDescriptor();
    }
}
