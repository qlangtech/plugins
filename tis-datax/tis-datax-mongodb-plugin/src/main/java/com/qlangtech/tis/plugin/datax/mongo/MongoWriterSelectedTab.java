package com.qlangtech.tis.plugin.datax.mongo;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;

/**
 * 用在writer 写入扩展类
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/8/30
 */
public class MongoWriterSelectedTab extends SelectedTab {

    @FormField(ordinal = 4, validate = {Validator.require})
    public UpsertSupport upsert;

    @TISExtension
    public static class DftDesc extends SelectedTab.DefaultDescriptor {

        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, SelectedTab postFormVals) {
            return true;
        }
    }
}
