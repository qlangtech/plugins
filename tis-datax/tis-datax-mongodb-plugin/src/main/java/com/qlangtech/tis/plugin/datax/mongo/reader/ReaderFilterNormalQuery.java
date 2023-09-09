package com.qlangtech.tis.plugin.datax.mongo.reader;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.datax.plugin.reader.mongodbreader.MongoDBReader;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import org.bson.Document;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/9/8
 * @see MongoDBReader.Task
 */
public class ReaderFilterNormalQuery extends ReaderFilter {

    private static final Logger logger = LoggerFactory.getLogger(ReaderFilterNormalQuery.class);

    @FormField(ordinal = 1, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String query;

    @Override
    public Document createFilter() {
        Document fitler = Document.parse(query);
        return fitler;
    }

    @TISExtension
    public static class DftDesc extends Descriptor<ReaderFilter> {
        @Override
        public String getDisplayName() {
            return "normalQuery";
        }

        public boolean validateQuery(IFieldErrorHandler msgHandler, Context context, String fieldName, String query) {
            try {
                Document queryFilter = Document.parse(query);
            } catch (Exception e) {
                logger.warn(e.getMessage(), e);
                msgHandler.addFieldError(context, fieldName, e.getMessage());
                return false;
            }
            return true;
        }


    }

}
