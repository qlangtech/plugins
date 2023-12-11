package com.qlangtech.tis.plugins.incr.flink.connector.elasticsearch7;

import com.alibaba.datax.common.exception.CommonErrorCode;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.util.Configuration;
import org.apache.commons.lang3.time.DateFormatUtils;

import java.util.TimeZone;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/12/8
 */
class DateCast {

    static String datetimeFormat = "yyyy-MM-dd HH:mm:ss";

    static String dateFormat = "yyyy-MM-dd";

    static String timeFormat = "HH:mm:ss";

    static String timeZone = "GMT+8";

    static TimeZone timeZoner = TimeZone.getTimeZone(DateCast.timeZone);

    static void init(final Configuration configuration) {
        DateCast.datetimeFormat = configuration.getString(
                "common.column.datetimeFormat", datetimeFormat);
        DateCast.timeFormat = configuration.getString(
                "common.column.timeFormat", timeFormat);
        DateCast.dateFormat = configuration.getString(
                "common.column.dateFormat", dateFormat);
        DateCast.timeZone = configuration.getString("common.column.timeZone",
                DateCast.timeZone);
        DateCast.timeZoner = TimeZone.getTimeZone(DateCast.timeZone);
        return;
    }

    static String asString(final DateColumn column) {
        if (null == column.asDate()) {
            return null;
        }

        switch (column.getSubType()) {
            case DATE:
                return DateFormatUtils.format(column.asDate(), DateCast.dateFormat,
                        DateCast.timeZoner);
            case TIME:
                return DateFormatUtils.format(column.asDate(), DateCast.timeFormat,
                        DateCast.timeZoner);
            case DATETIME:
                return DateFormatUtils.format(column.asDate(),
                        DateCast.datetimeFormat, DateCast.timeZoner);
            default:
                throw DataXException
                        .asDataXException(CommonErrorCode.CONVERT_NOT_SUPPORT,
                                "时间类型出现不支持类型，目前仅支持DATE/TIME/DATETIME。该类型属于编程错误，请反馈给DataX开发团队 .");
        }
    }
}
