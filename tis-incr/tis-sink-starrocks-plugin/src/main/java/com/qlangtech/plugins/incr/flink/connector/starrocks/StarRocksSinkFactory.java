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
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.plugin.datax.DataXDorisWriter;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.DBConfig;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.doris.DorisSourceFactory;
import com.qlangtech.tis.plugin.incr.TISSinkFactory;
import com.qlangtech.tis.realtime.transfer.DTO;
import com.starrocks.connector.flink.StarRocksSink;
import com.starrocks.connector.flink.table.StarRocksSinkOptions;
import com.starrocks.connector.flink.table.StarRocksSinkSemantic;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

import static com.starrocks.connector.flink.table.StarRocksSinkOptions.*;

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
        IDataxReader reader = dataxProcessor.getReader(null);

        DBConfig dbConfig = dsFactory.getDbConfig();
        AtomicReference<SinkFunction<DTO>> sinkFuncRef = new AtomicReference<>();
        final IDataxProcessor.TableAlias tabName = tableName;
        dbConfig.vistDbURL(false, (dbName, jdbcUrl) -> {
            List<ISelectedTab> tabs = reader.getSelectedTabs();
            Optional<ISelectedTab> selectedTab = tabs.stream().filter((tab) -> StringUtils.equals(tabName.getFrom(), tab.getName())).findFirst();
            if (!selectedTab.isPresent()) {
                throw new IllegalStateException("target table:" + tabName.getFrom()
                        + " can not find matched table in:["
                        + tabs.stream().map((t) -> t.getName()).collect(Collectors.joining(",")) + "]");
            }
            sinkFuncRef.set(createSinkFunction(dbName, selectedTab.get(), jdbcUrl, dsFactory));
        });

        Objects.requireNonNull(sinkFuncRef.get(), "sinkFunc can not be null");
        return sinkFuncRef.get();
    }

    private SinkFunction<DTO> createSinkFunction(
            String dbName, ISelectedTab tab, String jdbcUrl, DorisSourceFactory dsFactory) {
//import org.apache.flink.table.types.DataType;
        TableSchema.Builder schemaBuilder = TableSchema.builder();
        String[] fieldKeys = new String[tab.getCols().size()];
        int index = 0;
        for (ISelectedTab.ColMeta cm : tab.getCols()) {
            schemaBuilder.field(cm.getName(), mapFlinkColType(cm.getType()));
            fieldKeys[index++] = cm.getName();
        }

        return StarRocksSink.sink(
                // the table structure
                schemaBuilder.build(),
                // the sink options
                StarRocksSinkOptions.builder()
                        .withProperty(JDBC_URL.key(), jdbcUrl)
                        .withProperty(LOAD_URL.key(), dsFactory.loadUrl)
                        .withProperty(USERNAME.key(), dsFactory.getUserName())
                        .withProperty(PASSWORD.key(), dsFactory.getPassword())
                        .withProperty(TABLE_NAME.key(), tab.getName())
                        .withProperty(DATABASE_NAME.key(), dbName)
                        .withProperty(SINK_PROPERTIES_PREFIX + "column_separator", "\\x01")
                        .withProperty(SINK_PROPERTIES_PREFIX + "row_delimiter", "\\x02")
                        .withProperty(SINK_SEMANTIC.key(), StarRocksSinkSemantic.AT_LEAST_ONCE.getName())
                        .build()
                // set the slots with streamRowData
                , (slots, streamRowData) -> {
                    for (int i = 0; i < fieldKeys.length; i++) {
                        slots[i] = streamRowData.getAfter().get(fieldKeys[i]);
                    }
                }
        );
    }

    private org.apache.flink.table.types.DataType mapFlinkColType(ColumnMetaData.DataType type) {

        return type.accept(new ColumnMetaData.TypeVisitor<DataType>() {
            @Override
            public DataType longType(ColumnMetaData.DataType type) {
                return DataTypes.BIGINT();
            }

            @Override
            public DataType doubleType(ColumnMetaData.DataType type) {
                return DataTypes.DOUBLE();
            }

            @Override
            public DataType dateType(ColumnMetaData.DataType type) {
                return DataTypes.DATE();
            }

            @Override
            public DataType timestampType(ColumnMetaData.DataType type) {
                return DataTypes.TIMESTAMP();
            }

            @Override
            public DataType bitType(ColumnMetaData.DataType type) {
                return DataTypes.BOOLEAN();
            }

            @Override
            public DataType blobType(ColumnMetaData.DataType type) {
                return DataTypes.VARBINARY(type.columnSize);
            }

            @Override
            public DataType varcharType(ColumnMetaData.DataType type) {
                return DataTypes.VARCHAR(type.columnSize);
            }
        });

//        switch (type.type) {
//            case Types.BIT:
//            case Types.BOOLEAN:
//            case Types.TINYINT:
//                return DataTypes.TINYINT();
//            case Types.SMALLINT:
//                return "SMALLINT";
//            case Types.INTEGER:
//                return "int(11)";
//            case Types.BIGINT:
//                return "BIGINT(20)";
//            case Types.FLOAT:
//                return "FLOAT";
//            case Types.DOUBLE:
//                return "DOUBLE";
//            case Types.DECIMAL:
//                return "DECIMAL";
//            case Types.DATE:
//                return "DATE";
//            case Types.TIME:
//                return "TIME";
//            case Types.TIMESTAMP:
//                return "TIMESTAMP";
//            case Types.BLOB:
//            case Types.BINARY:
//            case Types.LONGVARBINARY:
//            case Types.VARBINARY:
//                return "BLOB";
//            case Types.VARCHAR:
//                return "VARCHAR(" + type.columnSize + ")";
//            default:
//                return "TINYTEXT";
//        }

        // return null;
    }
}
