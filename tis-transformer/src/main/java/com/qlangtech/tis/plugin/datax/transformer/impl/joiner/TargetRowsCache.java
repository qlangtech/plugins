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

        /**
         * 主表侧的 join key 是否存在 null 值
         * <p>null 在 SQL join 中本就不匹配，全量预加载模式下据此直接判为未命中</p>
         *
         * @return
         */
        public boolean hasNullPrimaryVal() {
            for (Object val : primaryVals) {
                if (val == null) {
                    return true;
                }
            }
            return false;
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

    /**
     * 是否为全量预加载模式：是则 JoinerUDF 在首次 evaluate 时触发一次 {@link #preload(BulkLoader)} 全量加载，
     * 之后所有记录只做内存查找，不再访问 DB
     *
     * @return
     */
    public boolean isFullPreload() {
        return false;
    }

    /**
     * 触发维度表全量加载，仅 {@link #isFullPreload()} 为 true 的实现需要支持
     *
     * @param loader 由 JoinerUDF 提供，负责执行批量查询并将结果逐行推给 {@link RowSink}
     */
    public void preload(BulkLoader loader) {
        throw new UnsupportedOperationException("preload is not supported by " + this.getClass().getSimpleName());
    }

    /**
     * 全量加载执行器：执行批量查询，将维度表逐行 emit 给 sink
     */
    public interface BulkLoader {
        void load(RowSink sink) throws Exception;
    }

    /**
     * 全量加载的行接收器
     */
    public interface RowSink {
        void accept(JoinCacheKey key, JoinCacheValue value);
    }

    @JSONField(serialize = false)
    @Override
    public Descriptor<TargetRowsCache> getDescriptor() {
        return Describable.super.getDescriptor();
    }
}
