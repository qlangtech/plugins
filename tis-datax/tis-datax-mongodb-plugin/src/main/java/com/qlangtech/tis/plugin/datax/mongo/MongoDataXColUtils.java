package com.qlangtech.tis.plugin.datax.mongo;

import com.alibaba.datax.common.element.BoolColumn;
import com.alibaba.datax.common.element.Column;
import com.alibaba.datax.common.element.DateColumn;
import com.alibaba.datax.common.element.DoubleColumn;
import com.alibaba.datax.common.element.LongColumn;
import com.alibaba.datax.common.element.StringColumn;
import com.google.common.base.Joiner;
import org.bson.BsonType;

import java.util.Date;
import java.util.List;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/9/11
 */
public class MongoDataXColUtils {
    public static Column createCol(MongoCMeta cMeta, Object val) {
        if (val == null) {
            //continue; 这个不能直接continue会导致record到目的端错位
            return new StringColumn(null);
        } else if (val instanceof Double) {
            //TODO deal with Double.isNaN()
            return new DoubleColumn((Double) val);
        } else if (val instanceof Boolean) {
            return (new BoolColumn((Boolean) val));
        } else if (val instanceof Date) {
            return (new DateColumn((Date) val));
        } else if (val instanceof Integer) {
            return (new LongColumn((Integer) val));
        } else if (val instanceof Long) {
            return (new LongColumn((Long) val));
        } else {

            if (cMeta.getMongoFieldType() == BsonType.ARRAY) {
                List array = (List) val;
                return (new StringColumn(Joiner.on(",").join(array)));
            } else {
                return (new StringColumn(val.toString()));
            }
        }
    }
}
