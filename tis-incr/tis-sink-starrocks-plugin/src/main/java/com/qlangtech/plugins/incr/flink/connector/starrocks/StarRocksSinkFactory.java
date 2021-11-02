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

package com.qlangtech.plugins.incr.flink.connector.starrocks;

import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.plugin.datax.DataXDorisWriter;
import com.qlangtech.tis.plugin.ds.DBConfig;
import com.qlangtech.tis.plugin.ds.doris.DorisSourceFactory;
import com.qlangtech.tis.plugin.incr.TISSinkFactory;
import com.qlangtech.tis.realtime.transfer.DTO;
import com.starrocks.connector.flink.StarRocksSink;
import com.starrocks.connector.flink.table.StarRocksSinkOptions;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;

import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * https://docs.starrocks.com/zh-cn/main/loading/Flink-connector-starrocks
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-10-31 20:11
 **/
public class StarRocksSinkFactory extends TISSinkFactory {
    @Override
    public SinkFunction<DTO> createSinkFunction(IDataxProcessor dataxProcessor) {

        Map<String, IDataxProcessor.TableAlias> tabAlias = dataxProcessor.getTabAlias();
        if (tabAlias == null || tabAlias.isEmpty()) {
            throw new IllegalStateException("has not set tables");
        }
        IDataxProcessor.TableAlias tableName = null;
        for (Map.Entry<String, IDataxProcessor.TableAlias> entry : tabAlias.entrySet()) {
            tableName = entry.getValue();
            break;
        }
        Objects.requireNonNull(tableName, "tableName can not be null");
        DataXDorisWriter dataXWriter = (DataXDorisWriter) dataxProcessor.getWriter(null);
        Objects.requireNonNull(dataXWriter, "dataXWriter can not be null");
        DorisSourceFactory dsFactory = dataXWriter.getDataSourceFactory();

        DBConfig dbConfig = dsFactory.getDbConfig();
        AtomicReference<SinkFunction<DTO>> sinkFuncRef = new AtomicReference<>();
        final IDataxProcessor.TableAlias tabName = tableName;
        dbConfig.vistDbURL(false, (dbName, jdbcUrl) -> {
            sinkFuncRef.set(createSinkFunction(dbName, tabName, jdbcUrl, dsFactory));
        });

        Objects.requireNonNull(sinkFuncRef.get(), "sinkFunc can not be null");
        return sinkFuncRef.get();
    }

    protected SinkFunction<DTO> createSinkFunction(
            String dbName, IDataxProcessor.TableAlias tableName, String jdbcUrl, DorisSourceFactory dsFactory) {
        return StarRocksSink.sink(
                // the table structure
                TableSchema.builder()
                        .field("score", DataTypes.INT())
                        .field("name", DataTypes.VARCHAR(20))
                        .build(),
                // the sink options
                StarRocksSinkOptions.builder()
                        .withProperty("jdbc-url", jdbcUrl)
                        .withProperty("load-url", dsFactory.loadUrl)
                        .withProperty("username", dsFactory.getUserName())
                        .withProperty("password", dsFactory.getPassword())
                        .withProperty("table-name", tableName.getTo())
                        .withProperty("database-name", dbName)
                        .withProperty("sink.properties.column_separator", "\\x01")
                        .withProperty("sink.properties.row_delimiter", "\\x02")
                        .build()
                // set the slots with streamRowData
                , (slots, streamRowData) -> {
//                    slots[0] = streamRowData.get;
//                    slots[1] = streamRowData.name;
                }
        );
    }
}
