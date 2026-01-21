package com.qlangtech.tis.plugin.datax.transformer.impl.joiner;

import com.alibaba.fastjson.annotation.JSONField;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;

/**
 * 目标记录是否开启缓存
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/1/15
 * @see JoinerSetMatchConditionAndCols
 */
public abstract class TargetRowsCache implements Describable<TargetRowsCache> {
    public abstract boolean isOn();

    @JSONField(serialize = false)
    @Override
    public Descriptor<TargetRowsCache> getDescriptor() {
        return Describable.super.getDescriptor();
    }
}
