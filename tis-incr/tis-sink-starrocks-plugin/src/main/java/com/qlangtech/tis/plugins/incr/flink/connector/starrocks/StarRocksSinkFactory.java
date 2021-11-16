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

package com.qlangtech.tis.plugins.incr.flink.connector.starrocks;

import com.alibaba.citrus.turbine.Context;
import com.google.common.collect.Maps;
import com.qlangtech.tis.datax.IDataXPluginMeta;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.DataXDorisWriter;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.DBConfig;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.doris.DorisSourceFactory;
import com.qlangtech.tis.plugin.incr.TISSinkFactory;
import com.qlangtech.tis.realtime.transfer.DTO;
import com.qlangtech.tis.realtime.transfer.UnderlineUtils;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.starrocks.connector.flink.StarRocksSink;
import com.starrocks.connector.flink.table.StarRocksSinkOptions;
import com.starrocks.connector.flink.table.StarRocksSinkSemantic;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.description.BlockElement;
import org.apache.flink.configuration.description.TextElement;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;

import java.lang.reflect.Field;
import java.util.*;
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

    public static final String DISPLAY_NAME_FLINK_CDC_SINK = "Flink-CDC-StarRocks-Sink";

    @FormField(ordinal = 0, type = FormFieldType.ENUM, validate = Validator.require)
    public String sinkSemantic;

    @FormField(ordinal = 1, type = FormFieldType.INT_NUMBER, validate = {})
    public Integer sinkConnectTimeout;

    @FormField(ordinal = 2, type = FormFieldType.INT_NUMBER, validate = {})
    public Long sinkBatchMaxSize;

    @FormField(ordinal = 3, type = FormFieldType.INT_NUMBER, validate = {})
    public Long sinkBatchMaxRows;

    @FormField(ordinal = 4, type = FormFieldType.INT_NUMBER, validate = {})
    public Long sinkBatchFlushInterval;

    @FormField(ordinal = 5, type = FormFieldType.INT_NUMBER, validate = {})
    public Long sinkMaxRetries;


    @FormField(ordinal = 6, type = FormFieldType.ENUM, validate = {Validator.require})
    public String columnSeparator;

    @FormField(ordinal = 7, type = FormFieldType.ENUM, validate = {Validator.require})
    public String rowDelimiter;

    public static List<Option> allColumnSeparator() {
        return Collections.singletonList(new Option(Separator.x01.name(), Separator.x01.name()));
    }

    public static List<Option> allRowDelimiter() {
        return Collections.singletonList(new Option(Separator.x02.name(), Separator.x02.name()));
    }

    private enum Separator {
        x01("\\x01"),
        x02("\\x02");

        private String val;

        private Separator(String val) {
            this.val = val;
        }

        private static Separator parse(String name) {
            for (Separator s : Separator.values()) {
                if (s.name().equalsIgnoreCase(name)) {
                    return s;
                }
            }
            throw new IllegalStateException("illegal seperator name:" + name);
        }

    }


    public static List<Option> allSinkSemantic() {
        return Arrays.stream(StarRocksSinkSemantic.values())
                .map((s) -> new Option(StringUtils.capitalize(s.getName()), s.getName()))
                .collect(Collectors.toList());
    }

    private static ConfigOption cfg(String cfgField) {
        try {
            cfgField = StringUtils.upperCase(UnderlineUtils.addUnderline(cfgField).toString());
            Field field = StarRocksSinkOptions.class.getField(cfgField);
            return (ConfigOption) field.get(null);
        } catch (Exception e) {
            throw new RuntimeException("field:" + cfgField, e);
        }
    }

    public static String dft(String cfgField) {
        return String.valueOf(cfg(cfgField).defaultValue());
    }

    public static String desc(String cfgField) {
        List<BlockElement> blocks = cfg(cfgField).description().getBlocks();
        for (BlockElement element : blocks) {
            return ((TextElement) element).getFormat();
        }
        return StringUtils.EMPTY;
    }


    @Override
    public Map<IDataxProcessor.TableAlias, SinkFunction<DTO>> createSinkFunction(IDataxProcessor dataxProcessor) {

        Map<IDataxProcessor.TableAlias, SinkFunction<DTO>> sinkFuncs = Maps.newHashMap();
        IDataxProcessor.TableAlias tableName = null;
        // Map<String, IDataxProcessor.TableAlias> tabAlias = dataxProcessor.getTabAlias();
        DataXDorisWriter dataXWriter = (DataXDorisWriter) dataxProcessor.getWriter(null);
        Objects.requireNonNull(dataXWriter, "dataXWriter can not be null");
        IDataxReader reader = dataxProcessor.getReader(null);
        List<ISelectedTab> tabs = reader.getSelectedTabs();

        DorisSourceFactory dsFactory = dataXWriter.getDataSourceFactory();
        DBConfig dbConfig = dsFactory.getDbConfig();

        for (Map.Entry<String, IDataxProcessor.TableAlias> tabAliasEntry : dataxProcessor.getTabAlias().entrySet()) {
            tableName = tabAliasEntry.getValue();

            Objects.requireNonNull(tableName, "tableName can not be null");
            if (StringUtils.isEmpty(tableName.getFrom())) {
                throw new IllegalStateException("tableName.getFrom() can not be empty");
            }


            AtomicReference<SinkFunction<DTO>> sinkFuncRef = new AtomicReference<>();
            final IDataxProcessor.TableAlias tabName = tableName;
            AtomicReference<Object[]> exceptionLoader = new AtomicReference<>();
            final String targetTabName = tableName.getTo();
            dbConfig.vistDbURL(false, (dbName, jdbcUrl) -> {
                try {
                    Optional<ISelectedTab> selectedTab = tabs.stream()
                            .filter((tab) -> StringUtils.equals(tabName.getFrom(), tab.getName())).findFirst();
                    if (!selectedTab.isPresent()) {
                        throw new IllegalStateException("target table:" + tabName.getFrom()
                                + " can not find matched table in:["
                                + tabs.stream().map((t) -> t.getName()).collect(Collectors.joining(",")) + "]");
                    }
                    /**
                     * 需要先初始化表starrocks目标库中的表
                     */
                    dataXWriter.initWriterTable(targetTabName, Collections.singletonList(jdbcUrl));

                    sinkFuncRef.set(createSinkFunction(dbName, targetTabName, selectedTab.get(), jdbcUrl, dsFactory));

                } catch (Throwable e) {
                    exceptionLoader.set(new Object[]{jdbcUrl, e});
                }
            });
            if (exceptionLoader.get() != null) {
                Object[] error = exceptionLoader.get();
                throw new RuntimeException((String) error[0], (Throwable) error[1]);
            }
            Objects.requireNonNull(sinkFuncRef.get(), "sinkFunc can not be null");
            sinkFuncs.put(tableName, sinkFuncRef.get());
        }
//        if (tabAlias == null || tabAlias.isEmpty()) {
//            throw new IllegalStateException("has not set tables");
//        }
//        IDataxProcessor.TableAlias tableName = null;
//        for (Map.Entry<String, IDataxProcessor.TableAlias> entry : tabAlias.entrySet()) {
//            tableName = entry.getValue();
//            break;
//        }


        if (sinkFuncs.size() < 1) {
            throw new IllegalStateException("size of sinkFuncs can not be small than 1");
        }
        return sinkFuncs;
    }

    private SinkFunction<DTO> createSinkFunction(
            String dbName, final String targetTabName, ISelectedTab tab, String jdbcUrl, DorisSourceFactory dsFactory) {
//import org.apache.flink.table.types.DataType;
        TableSchema.Builder schemaBuilder = TableSchema.builder();
        String[] fieldKeys = new String[tab.getCols().size()];
        if (fieldKeys.length < 1) {
            throw new IllegalArgumentException("fieldKeys.length can not small than 1");
        }
        int index = 0;
        for (ISelectedTab.ColMeta cm : tab.getCols()) {
            schemaBuilder.field(cm.getName(), mapFlinkColType(cm.getType()));
            fieldKeys[index++] = cm.getName();
        }

        return StarRocksSink.sink(
                // the table structure
                schemaBuilder.build(),
                // the sink options
                createRocksSinkOptions(dbName, targetTabName, jdbcUrl, dsFactory)
                // set the slots with streamRowData
                , (slots, streamRowData) -> {
                    if (streamRowData.getEventType() == DTO.EventType.DELETE) {
                        return;
                    }
                    for (int i = 0; i < fieldKeys.length; i++) {
                        slots[i] = streamRowData.getAfter().get(fieldKeys[i]);
                    }
                }
        );
    }

    private StarRocksSinkOptions createRocksSinkOptions(String dbName, String targetTabName, String jdbcUrl, DorisSourceFactory dsFactory) {
        StarRocksSinkOptions.Builder builder = StarRocksSinkOptions.builder()
                .withProperty(JDBC_URL.key(), jdbcUrl)
                .withProperty(LOAD_URL.key(), dsFactory.getLoadUrls().stream().collect(Collectors.joining(";")))
                .withProperty(TABLE_NAME.key(), targetTabName)
                .withProperty(DATABASE_NAME.key(), dbName)
                .withProperty(SINK_PROPERTIES_PREFIX + "column_separator", Separator.parse(this.columnSeparator).val)
                .withProperty(SINK_PROPERTIES_PREFIX + "row_delimiter", Separator.parse(this.rowDelimiter).val)
                .withProperty(SINK_SEMANTIC.key(), StarRocksSinkSemantic.fromName(this.sinkSemantic).getName())
                .withProperty(USERNAME.key(), dsFactory.getUserName());


        if (this.sinkConnectTimeout != null) {
            builder.withProperty(SINK_CONNECT_TIMEOUT.key(), String.valueOf(this.sinkConnectTimeout));
        }
        if (this.sinkBatchMaxSize != null) {
            builder.withProperty(SINK_BATCH_MAX_SIZE.key(), String.valueOf(this.sinkBatchMaxSize));
        }
        if (this.sinkBatchMaxRows != null) {
            builder.withProperty(SINK_BATCH_MAX_ROWS.key(), String.valueOf(this.sinkBatchMaxRows));
        }
        if (this.sinkBatchFlushInterval != null) {
            builder.withProperty(SINK_BATCH_FLUSH_INTERVAL.key(), String.valueOf(this.sinkBatchFlushInterval));
        }
        if (this.sinkMaxRetries != null) {
            builder.withProperty(SINK_MAX_RETRIES.key(), String.valueOf(this.sinkMaxRetries));
        }

        //if (StringUtils.isNotEmpty(dsFactory.getPassword())) {
        builder.withProperty(PASSWORD.key(), StringUtils.trimToEmpty(dsFactory.getPassword()));
        //}
        return builder.build();
    }

    private org.apache.flink.table.types.DataType mapFlinkColType(ColumnMetaData.DataType type) {
        if (type == null) {
            throw new IllegalArgumentException("param type can not be null");
        }
        return type.accept(new ColumnMetaData.TypeVisitor<DataType>() {
            @Override
            public DataType intType(ColumnMetaData.DataType type) {
                return DataTypes.INT();
            }

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
    }

    @TISExtension
    public static class DefaultSinkFunctionDescriptor extends BaseSinkFunctionDescriptor {
        @Override
        public String getDisplayName() {
            return DISPLAY_NAME_FLINK_CDC_SINK;
        }

        public boolean validateColumnSeparator(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            return validateRowDelimiter(msgHandler, context, fieldName, value);
        }

        public boolean validateRowDelimiter(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
//            if (StringUtils.length(StringEscapeUtils.unescapeJava(value)) != 1) {
//                msgHandler.addFieldError(context, fieldName, "分隔符长度必须为1");
//                return false;
//            }
            return true;
        }

        @Override
        protected IDataXPluginMeta.EndType getTargetType() {
            return IDataXPluginMeta.EndType.StarRocks;
        }
    }
}
