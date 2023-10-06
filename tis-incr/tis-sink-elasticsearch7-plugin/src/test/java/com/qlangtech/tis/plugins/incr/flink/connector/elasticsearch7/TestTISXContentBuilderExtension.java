package com.qlangtech.tis.plugins.incr.flink.connector.elasticsearch7;

import com.google.common.collect.Maps;
import org.apache.flink.table.data.DecimalData;
import org.elasticsearch.common.bytes.BytesReference;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.elasticsearch.common.xcontent.XContentType;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.Map;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/10/5
 * @see TISXContentBuilderExtension
 */
public class TestTISXContentBuilderExtension {

    @BeforeClass
    public static void initialize() {
        TISXContentBuilderExtension.load();
    }

    @Test
    public void testTimeStampSerialize() throws Exception {

        Map<String, Object> source = Maps.newHashMap();

//yyyy-[m]m-[d]d hh:mm:ss
        Timestamp stamp = Timestamp.valueOf("2023-11-9 11:12:59");
        source.put("start_time", stamp);
        Date date = Date.valueOf("2022-11-9");
        source.put("start_date", date);
//hh:mm:ss
        Time time = Time.valueOf("11:22:59");
        source.put("occur_time", time);
        DecimalData decimalVal = DecimalData.fromUnscaledLong(9999, 4, 2);
        source.put("decial_field", decimalVal);

        XContentBuilder builder = XContentFactory.contentBuilder(XContentType.JSON);
        builder.map(source);
        BytesReference bytes = BytesReference.bytes(builder);
        Assert.assertNotNull(bytes);
        // System.out.println(  bytes.utf8ToString() );

        Assert.assertEquals("{\"start_time\":\"2023-11-09T11:12:59.000+08:00\",\"occur_time\":\"1970-01-01T11:22:59.000+08:00\",\"decial_field\":99.99,\"start_date\":\"2022-11-09T00:00:00.000+08:00\"}", bytes.utf8ToString());

    }
}
