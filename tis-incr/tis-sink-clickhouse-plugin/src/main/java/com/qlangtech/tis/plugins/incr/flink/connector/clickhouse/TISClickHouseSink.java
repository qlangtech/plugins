/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.tis.plugins.incr.flink.connector.clickhouse;

import com.qlangtech.tis.realtime.transfer.DTO;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import ru.ivi.opensource.flinkclickhousesink.ClickHouseSink;
import ru.ivi.opensource.flinkclickhousesink.applied.ClickHouseSinkManager;
import ru.ivi.opensource.flinkclickhousesink.applied.Sink;

import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-12-01 14:23
 **/
public class TISClickHouseSink extends RichSinkFunction<DTO> {

    private static final Logger logger = LoggerFactory.getLogger(ClickHouseSink.class);

    private static final Object DUMMY_LOCK = new Object();

    private final Properties localProperties;
    private final Map<String, String> globalParams;

    private volatile static transient ClickHouseSinkManager sinkManager;
    private transient Sink sink;

    private final List<ColMeta> colsMeta;

    public TISClickHouseSink(Map<String, String> globalParams, Properties properties, List<ColMeta> colsMeta) {
        this.localProperties = properties;
        this.globalParams = globalParams;
        this.colsMeta = colsMeta;
        if (globalParams == null || globalParams.isEmpty()) {
            throw new IllegalArgumentException("globalParams can not be empty");
        }
    }

    @Override
    public void open(Configuration config) {
        if (sinkManager == null) {
            synchronized (DUMMY_LOCK) {
                if (sinkManager == null) {
//                    Map<String, String> params = getRuntimeContext()
//                            .getExecutionConfig()
//                            .getGlobalJobParameters()
//                            .toMap();

                    sinkManager = new ClickHouseSinkManager(globalParams);
                }
            }
        }

        sink = sinkManager.buildSink(localProperties);
    }


    @Override
    public void invoke(DTO dto, Context context) {
        String recordAsCSV = convertCSV(dto);
        try {
            sink.put(recordAsCSV);
        } catch (Exception e) {
            logger.error("Error while sending data to ClickHouse, record = {}", recordAsCSV, e);
            throw new RuntimeException(recordAsCSV, e);
        }
    }

    private void strVal(final Map<String, Object> afterVals, ColMeta cm, StringBuffer result) {
        Object val = afterVals.get(cm.getKey());
        if (val != null) {
            result.append("'").append(val).append("'");
        } else {
            result.append("null");
        }
    }

    private void numericVal(final Map<String, Object> afterVals, ColMeta cm, StringBuffer result) {
        Object val = afterVals.get(cm.getKey());
        if (val != null) {
            result.append(val);
        } else {
            result.append("null");
        }
    }

    private String convertCSV(DTO dto) {
        final Map<String, Object> afterVals = dto.getAfter();
        StringBuffer result = new StringBuffer("(");
        int size = colsMeta.size();
        int index = 0;
        for (ColMeta cm : colsMeta) {
            switch (cm.getType().type) {
                case Types.INTEGER:
                case Types.TINYINT:
                case Types.SMALLINT:
                case Types.BIGINT:
                case Types.FLOAT:
                case Types.DOUBLE:
                case Types.DECIMAL:
                case Types.DATE:
                case Types.TIME:
                case Types.TIMESTAMP:
                case Types.BIT:
                case Types.BOOLEAN:
                    numericVal(afterVals, cm, result);
                    break;
                case Types.BLOB:
                case Types.BINARY:
                case Types.LONGVARBINARY:
                case Types.VARBINARY:
                default:
                    strVal(afterVals, cm, result);
            }
            if (index++ < (size - 1)) {
                result.append(",");
            }
        }
        result.append(")");
        return result.toString();

    }

    @Override
    public void close() throws Exception {
        if (sink != null) {
            sink.close();
        }

        if (sinkManager != null) {
            if (!sinkManager.isClosed()) {
                synchronized (DUMMY_LOCK) {
                    if (!sinkManager.isClosed()) {
                        sinkManager.close();
                    }
                }
            }
        }

        super.close();
    }
}
