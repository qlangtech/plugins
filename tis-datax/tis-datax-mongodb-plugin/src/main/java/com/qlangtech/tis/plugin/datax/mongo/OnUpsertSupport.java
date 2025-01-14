package com.qlangtech.tis.plugin.datax.mongo;

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.alibaba.datax.plugin.writer.mongodbwriter.MongoKeyConstant;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/8/30
 */
public class OnUpsertSupport extends UpsertSupport {

    @FormField(ordinal = 0, type = FormFieldType.ENUM, validate = {Validator.require})
    public String upsertKey;

    @Override
    public boolean supportUpset() {
        return true;
    }

    @Override
    public JSONObject getUpsetCfg() {
        // {"isUpsert":true,"upsertKey":"unique_id"}
        JSONObject conf = new JSONObject();
        conf.put(MongoKeyConstant.IS_REPLACE, true);
        conf.put(MongoKeyConstant.UNIQUE_KEY, this.upsertKey);
        return conf;
    }


    @TISExtension
    public static class DftDesc extends Descriptor<UpsertSupport> {
        @Override
        public String getDisplayName() {
            return SWITCH_ON;
        }
    }

}
