/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qlangtech.tis.plugins.incr.flink.chunjun.kafka.sink;

import com.alibaba.citrus.turbine.Context;
import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.jdbc.TableCols;
import com.dtstack.chunjun.connector.jdbc.conf.JdbcConf;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.sink.JdbcOutputFormat;
import com.dtstack.chunjun.connector.kafka.conf.KafkaConf;
import com.dtstack.chunjun.connector.kafka.converter.KafkaColumnConverter;
import com.dtstack.chunjun.connector.kafka.serialization.RowSerializationSchema;
import com.dtstack.chunjun.connector.kafka.sink.KafkaProducer;
import com.dtstack.chunjun.connector.kafka.sink.KafkaSinkFactory;
import com.dtstack.chunjun.converter.ISerializationConverter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.compiler.incr.ICompileAndPackage;
import com.qlangtech.tis.compiler.streamcode.CompileAndPackage;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IStreamTableMeta;
import com.qlangtech.tis.datax.TableAlias;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.datax.SelectedTabExtend;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugins.datax.kafka.writer.DataXKafkaWriter;
import com.qlangtech.tis.plugins.datax.kafka.writer.KafkaSelectedTab;
import com.qlangtech.tis.plugins.incr.flink.cdc.AbstractRowDataMapper;
import com.qlangtech.tis.plugins.incr.flink.chunjun.kafka.format.FormatFactory;
import com.qlangtech.tis.plugins.incr.flink.chunjun.kafka.format.FormatFactory.BasicFormatDescriptor;
import com.qlangtech.tis.plugins.incr.flink.chunjun.table.ChunjunTableSinkFactory;
import com.qlangtech.tis.plugins.incr.flink.connector.ChunjunSinkFactory;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.util.Preconditions;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-03-17 12:31
 **/
public class ChujunKafkaSinkFactory extends ChunjunSinkFactory {

    @FormField(ordinal = 0, validate = {Validator.require})
    public FormatFactory format;

    public static List<? extends Descriptor> supportedFormats(List<? extends Descriptor> descs) {
        if (CollectionUtils.isEmpty(descs)) {
            return Collections.emptyList();
        }
        return descs.stream().filter((desc) ->
                ((BasicFormatDescriptor) desc).getEndType().sinkSupport).collect(Collectors.toList());
    }

    @Override
    protected boolean supportUpsetDML() {
        return true;
    }

    @Override
    protected CreateChunjunSinkFunctionResult createSinFunctionResult(
            IDataxProcessor dataxProcessor, SelectedTab selectedTab, final String targetTabName, boolean shallInitSinkTable) {

        CreateChunjunSinkFunctionResult sinkFuncRef = new CreateChunjunSinkFunctionResult();
        sinkFuncRef.setPrimaryKeys(selectedTab.getPrimaryKeys());
        KafkaSelectedTab kfkTable = (KafkaSelectedTab) selectedTab;

        DataXKafkaWriter dataXWriter = (DataXKafkaWriter) dataxProcessor.getWriter(null);

        KafkaConf kafkaConf = new KafkaConf();
        //
        kafkaConf.setPartitionAssignColumns(kfkTable.partitionFields);
        kafkaConf.setTableFields(
                kfkTable.getCols().stream().map((col) -> col.getName()).collect(Collectors.toList()));
        kafkaConf.setTableName(targetTabName);
        kafkaConf.setTopic(dataXWriter.topic);

        kafkaConf.setProducerSettings(dataXWriter.buildKafkaConfig());
        TableAlias tableAlias = TableAlias.create(selectedTab.getName(), targetTabName);
        SyncConf syncConf = createSyncConf(selectedTab, tableAlias, () -> {
            Map<String, Object> params = Maps.newHashMap();
            return params;
        }, dataXWriter);

        KafkaSinkFactory sinkFactory = new KafkaSinkFactory(syncConf, kafkaConf) {
            @Override
            protected KafkaProducer createKafkaProducer(Properties props, RowSerializationSchema rowSerializationSchema) {

                TISKafkaProducer kafkaProducer = new TISKafkaProducer(
                        kafkaConf.getTopic(),
                        rowSerializationSchema,
                        props,
                        getSemantic(),
                        FlinkKafkaProducer.DEFAULT_KAFKA_PRODUCERS_POOL_SIZE);

                sinkFuncRef.setSinkFunction(kafkaProducer);
                return kafkaProducer;
            }

            @Override
            protected SerializationSchema<RowData> getValSerializationSchema() {

                Function<FieldConf, ISerializationConverter<Map<String, Object>>> serializationConverterFactory = getSerializationConverterFactory();
                Preconditions.checkNotNull(serializationConverterFactory, "serializationConverterFactory can not be null");

                KafkaColumnConverter valConverter = KafkaColumnConverter.create(this.syncConf, kafkaConf, serializationConverterFactory);
                List<KafkaSerializationConverter> colConvert = valConverter.getExternalConverters();
                TableSchema tableSchema = ChunjunTableSinkFactory.ChunjunStreamTableSink.createTableSchema(selectedTab.getPrimaryKeys(),
                        colConvert.stream().map((c) -> c.flinkCol).collect(Collectors.toList()));


                return Objects.requireNonNull(format.createEncodingFormat(targetTabName), "encodingFormat can not be null")
                        .createRuntimeEncoder(null, tableSchema.toRowDataType());
            }


            @Override
            protected Function<FieldConf, ISerializationConverter<Map<String, Object>>> getSerializationConverterFactory() {
                return (fieldCfg) -> {
                    IColMetaGetter cmGetter = IColMetaGetter.create(fieldCfg.getName(), fieldCfg.getType());
                    FlinkCol flinkCol = AbstractRowDataMapper.mapFlinkCol(cmGetter, fieldCfg.getIndex());
                    return new KafkaSerializationConverter(flinkCol);
                };
            }
        };


        sinkFuncRef.setSinkFactory(sinkFactory);
        sinkFuncRef.initialize();
        sinkFuncRef.setSinkCols(new TableCols(selectedTab.getCols()));

        //Objects.requireNonNull(sinkFuncRef.get(), "sinkFunc can not be null");
        sinkFuncRef.setParallelism(this.parallelism);
        return sinkFuncRef;
    }


    public static List<SemanticOption> supportSemantic() {

//        "enum": [
//        {
//            "label": "Exactly-Once",
//                "val": "exactly-once"
//        },
//        {
//            "label": "At-Least-Once",
//                "val": "at-least-once"
//        }
//      ]
        return Lists.newArrayList(new SemanticOption("None", "none", FlinkKafkaProducer.Semantic.NONE)
                , new SemanticOption("Exactly-Once", "exactly-once", FlinkKafkaProducer.Semantic.EXACTLY_ONCE)
                , new SemanticOption("At-Least-Once", "at-least-once", FlinkKafkaProducer.Semantic.AT_LEAST_ONCE));
    }

    private static class SemanticOption extends Option {
        private final FlinkKafkaProducer.Semantic semantic;

        public SemanticOption(String name, Object value, FlinkKafkaProducer.Semantic semantic) {
            super(name, value);
            this.semantic = semantic;
        }
    }

    private FlinkKafkaProducer.Semantic getSemantic() {
        for (SemanticOption opt : ChujunKafkaSinkFactory.supportSemantic()) {
            if (this.semantic.equals(opt.getValue())) {
                return opt.semantic;
            }
        }
        throw new IllegalStateException("illegal semantic:" + this.semantic);
    }


    @Override
    protected Object parseType(IColMetaGetter cm) {
        return cm.getType();
    }


    @Override
    protected Class<? extends JdbcDialect> getJdbcDialectClass() {
        throw new UnsupportedOperationException();
    }

    @Override
    protected JdbcOutputFormat createChunjunOutputFormat(TableAlias tableAlias, DataSourceFactory dsFactory, JdbcConf jdbcConf) {
        return null;
    }

    @Override
    protected void initChunjunJdbcConf(JdbcConf jdbcConf) {

    }

    @Override
    public IStreamTableMeta getStreamTableMeta(TableAlias tableName) {

        // DataxProcessor dataXProcessor = DataxProcessor.load(null, this.dataXName);
        //  DataxWriter writer = DataxWriter.load(null, this.dataXName);// dataXProcessor.getWriter(null);
        DataxReader reader = DataxReader.load(null, this.dataXName);

        List<ISelectedTab> tabs = reader.getSelectedTabs();

        Optional<ISelectedTab> find = tabs.stream()
                .filter((f) -> StringUtils.equals(f.getName(), tableName.getFrom())).findFirst();
        if (!find.isPresent()) {
            throw new IllegalStateException("target table:" + tableName + " can not find in reader selectedTabs:"
                    + tabs.stream().map((t) -> t.getName()).collect(Collectors.joining(",")));
        }

        return new IStreamTableMeta() {
            @Override
            public List<IColMetaGetter> getColsMeta() {
                return find.get().getCols().stream().map((col) -> (IColMetaGetter) col).collect(Collectors.toList());
            }
        };
    }

    @Override
    public ICompileAndPackage getCompileAndPackageManager() {
        return new CompileAndPackage(Sets.newHashSet(ChujunKafkaSinkFactory.class));
    }


    @TISExtension
    public static class DefaultDesc extends BasicChunjunSinkDescriptor {
        @Override
        protected EndType getTargetType() {
            return EndType.Kafka;
        }

        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {

            ChujunKafkaSinkFactory sinkFactory = postFormVals.newInstance();
            DataxReader dataxReader
                    = DataxReader.load((IPluginContext) msgHandler, msgHandler.getCollectionName().getPipelineName());
            // List<ISelectedTab> tabs = dataxReader.getSelectedTabs();
            if (!sinkFactory.validate(msgHandler, context, FormatFactory.KEY_FIELD_FORMAT, dataxReader)) {
                return false;
            }
            return super.validateAll(msgHandler, context, postFormVals);
        }

        @Override
        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            return super.verify(msgHandler, context, postFormVals);
        }

    }

    private boolean validate(IControlMsgHandler msgHandler, Context context, String fieldName, DataxReader dataxReader) {
        return this.format.validateFormtField(msgHandler, context, fieldName, dataxReader);
    }
}
