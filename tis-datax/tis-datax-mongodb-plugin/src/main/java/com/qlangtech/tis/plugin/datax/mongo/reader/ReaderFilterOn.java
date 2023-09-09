package com.qlangtech.tis.plugin.datax.mongo.reader;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.datax.plugin.reader.mongodbreader.KeyConstant;
import com.alibaba.datax.plugin.reader.mongodbreader.MongoDBReader;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import org.apache.commons.lang.StringUtils;
import org.bson.Document;
import org.bson.types.ObjectId;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/9/8
 * @see MongoDBReader.Task
 */
public class ReaderFilterOn extends ReaderFilter {

    @FormField(ordinal = 1, type = FormFieldType.ENUM, validate = {Validator.require})
    public Boolean usingObjectId;

    @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT, validate = {})
    public String lowerBound;
    @FormField(ordinal = 3, type = FormFieldType.INPUTTEXT, validate = {})
    public String upperBound;

    @Override
    public Document createFilter() {
        Document fitler = super.createFilter();

        Document filter = new Document();
        if (StringUtils.isEmpty(lowerBound)) {
            if (StringUtils.isNotEmpty(upperBound)) {
                filter.append(KeyConstant.MONGO_PRIMARY_ID //
                        , new Document("$lt", usingObjectId ? new ObjectId(upperBound) : upperBound));
            }
        } else if (StringUtils.isEmpty(upperBound) && StringUtils.isNotEmpty(lowerBound)) {
            filter.append(KeyConstant.MONGO_PRIMARY_ID //
                    , new Document("$gte", usingObjectId ? new ObjectId(lowerBound) : lowerBound));
        } else {
            filter.append(KeyConstant.MONGO_PRIMARY_ID //
                    , new Document("$gte", usingObjectId ? new ObjectId(lowerBound) : lowerBound) //
                            .append("$lt", usingObjectId ? new ObjectId(upperBound) : upperBound));
        }

        return fitler;
    }

    @TISExtension
    public static class DftDesc extends Descriptor<ReaderFilter> {

        @Override
        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            return this.validateAll(msgHandler, context, postFormVals);
        }

        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {

            ReaderFilterOn filter = postFormVals.newInstance();
            if (StringUtils.isEmpty(filter.lowerBound) && StringUtils.isEmpty(filter.upperBound)) {

                msgHandler.addErrorMessage(context, "查询区间不能都为空，上下区间至少填一项");
                return false;
            }

            return true;
        }

        @Override
        public String getDisplayName() {
            return "range";
        }
    }

}
