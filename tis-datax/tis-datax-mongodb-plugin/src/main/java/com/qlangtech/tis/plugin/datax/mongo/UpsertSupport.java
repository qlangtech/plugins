package com.qlangtech.tis.plugin.datax.mongo;

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.extension.Describable;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/8/30
 * @see OffUpsertSupport
 * @see OnUpsertSupport
 */
public abstract class UpsertSupport implements Describable<UpsertSupport> {

    public abstract boolean supportUpset();

    public JSONObject getUpsetCfg() {
        throw new UnsupportedOperationException();
    }
}
