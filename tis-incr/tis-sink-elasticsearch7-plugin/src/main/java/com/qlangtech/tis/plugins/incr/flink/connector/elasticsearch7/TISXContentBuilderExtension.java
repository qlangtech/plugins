package com.qlangtech.tis.plugins.incr.flink.connector.elasticsearch7;


import com.alibaba.datax.plugin.writer.elasticsearchwriter.DataConvertUtils;
import com.alibaba.datax.plugin.writer.elasticsearchwriter.ESColumn;
import com.google.common.collect.Maps;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentBuilderExtension;

import java.util.Collections;
import java.util.Map;
import java.util.function.Function;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/10/5
 */
public class TISXContentBuilderExtension implements XContentBuilderExtension {
    @Override
    public Map<Class<?>, XContentBuilder.Writer> getXContentWriters() {
        Map<Class<?>, XContentBuilder.Writer> writerMap = Maps.newHashMap();

        XContentBuilder.Writer timeWriter = (builder, val) -> {
            builder.timeValue(val);
        };

        writerMap.put(java.sql.Timestamp.class, timeWriter);

        writerMap.put(java.sql.Time.class, timeWriter);

        writerMap.put(java.sql.Date.class, timeWriter);

        writerMap.put(Boolean.class, (b, v) -> {
            b.value((Boolean) v);
        });

        writerMap.put(org.apache.flink.table.data.DecimalData.class, (builder, val) -> {
            builder.value(((org.apache.flink.table.data.DecimalData) val).toBigDecimal());
        });

        return writerMap;
    }

    public static void load() {

        ClassLoader current = Thread.currentThread().getContextClassLoader();
        try {
            Thread.currentThread().setContextClassLoader(ElasticSearchSinkFactory.class.getClassLoader());
            System.out.println(XContentBuilder.class);
        } finally {
            Thread.currentThread().setContextClassLoader(current);
        }

    }

    @Override
    public Map<Class<?>, XContentBuilder.HumanReadableTransformer> getXContentHumanReadableTransformers() {
        return Collections.emptyMap();
    }

    private static final ESColumn DATE_COLUMN_META;

    static {
        DATE_COLUMN_META = new ESColumn();
    }

    @Override
    public Map<Class<?>, Function<Object, Object>> getDateTransformers() {
        Map<Class<?>, Function<Object, Object>> transformers = Maps.newHashMap();
        transformers.put(java.sql.Timestamp.class, (timestamp) -> {
            return DataConvertUtils.getDateStr(DATE_COLUMN_META, new DateColumn((java.sql.Timestamp) timestamp));
        });
        transformers.put(java.sql.Time.class, (time) -> {
            return DataConvertUtils.getDateStr(DATE_COLUMN_META, new DateColumn((java.sql.Time) time));
        });

        transformers.put(java.sql.Date.class, (time) -> {
            return DataConvertUtils.getDateStr(DATE_COLUMN_META, new DateColumn((java.sql.Date) time));
        });

        return transformers;
    }


}
