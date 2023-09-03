package com.qlangtech.tis.plugin.datax.mongo;

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.plugin.ds.CMeta;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/9/3
 */
public class MongoCMetaCreatorFactory implements CMeta.ElementCreatorFactory{
    @Override
    public CMeta create(JSONObject targetCol) {

        MongoCMeta cMeta = new MongoCMeta();

        return cMeta;
    }
}
