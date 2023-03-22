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

import com.dtstack.chunjun.conf.FieldConf;
import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.jdbc.TableCols;
import com.dtstack.chunjun.connector.jdbc.conf.JdbcConf;
import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.sink.JdbcOutputFormat;
import com.dtstack.chunjun.connector.kafka.conf.KafkaConf;
import com.dtstack.chunjun.connector.kafka.converter.KafkaColumnConverter;
import com.dtstack.chunjun.connector.kafka.partitioner.CustomerFlinkPartition;
import com.dtstack.chunjun.connector.kafka.serialization.RowSerializationSchema;
import com.dtstack.chunjun.connector.kafka.sink.KafkaProducer;
import com.dtstack.chunjun.connector.kafka.sink.KafkaSinkFactory;
import com.dtstack.chunjun.converter.ISerializationConverter;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.compiler.incr.ICompileAndPackage;
import com.qlangtech.tis.compiler.streamcode.CompileAndPackage;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.datax.IncrSelectedTabExtend;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugins.datax.kafka.writer.DataXKafkaWriter;
import com.qlangtech.tis.plugins.datax.kafka.writer.KafkaSelectedTab;
import com.qlangtech.tis.plugins.incr.flink.cdc.AbstractRowDataMapper;
import com.qlangtech.tis.plugins.incr.flink.connector.ChunjunSinkFactory;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.util.Preconditions;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-03-17 12:31
 **/
public class ChujunKafkaSinkFactory extends ChunjunSinkFactory {
    @Override
    protected boolean supportUpsetDML() {
        return true;
    }

    @Override
    protected CreateChunjunSinkFunctionResult createSinFunctionResult(
            IDataxProcessor dataxProcessor, SelectedTab selectedTab, final String targetTabName, boolean shallInitSinkTable) {

        CreateChunjunSinkFunctionResult sinkFuncRef = new CreateChunjunSinkFunctionResult();
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

        SyncConf syncConf = createSyncConf(selectedTab, () -> {
            Map<String, Object> params = Maps.newHashMap();
            return params;
        });

        KafkaSinkFactory sinkFactory = new KafkaSinkFactory(syncConf, kafkaConf) {
            @Override
            protected KafkaProducer createKafkaProducer(Properties props, RowSerializationSchema rowSerializationSchema) {

                TISKafkaProducer kafkaProducer = new TISKafkaProducer(
                        kafkaConf.getTopic(),
                        rowSerializationSchema,
                        props,
                        FlinkKafkaProducer.Semantic.AT_LEAST_ONCE,
                        FlinkKafkaProducer.DEFAULT_KAFKA_PRODUCERS_POOL_SIZE);

                sinkFuncRef.setSinkFunction(kafkaProducer);
                return kafkaProducer;
            }

            @Override
            protected Function<FieldConf, ISerializationConverter<Map<String, Object>>> getSerializationConverterFactory() {
                return (fieldCfg) -> {
                    IColMetaGetter cmGetter = IColMetaGetter.create(fieldCfg.getName(), fieldCfg.getType());
                    //colsMetas.add(cmGetter);
                    FlinkCol flinkCol = AbstractRowDataMapper.mapFlinkCol(cmGetter, fieldCfg.getIndex());
                    return new KafkaSerializationConverter(flinkCol);
                };
            }

            @Override
            protected RowSerializationSchema createRowSerializationSchema(KafkaColumnConverter keyConverter) {

                Function<FieldConf, ISerializationConverter<Map<String, Object>>> serializationConverterFactory = getSerializationConverterFactory();
                Preconditions.checkNotNull(serializationConverterFactory, "serializationConverterFactory can not be null");
                return new RowSerializationSchema(
                        kafkaConf,
                        new CustomerFlinkPartition<>(),
                        keyConverter,
                        KafkaColumnConverter.create(this.syncConf, kafkaConf, serializationConverterFactory)) {
                    @Override
                    public Map<String, Object> createRowVals(String tableName, Map<String, Object> data) {
                        return DataXKafkaWriter.createRowVals(tableName, data);
                    }
                };
            }
        };


        sinkFuncRef.setSinkFactory(sinkFactory);
        sinkFuncRef.initialize();
        sinkFuncRef.setSinkCols(new TableCols(selectedTab.getCols()));
        // DBConfig dbConfig = dsFactory.getDbConfig();
//        dbConfig.vistDbURL(false, (dbName, dbHost, jdbcUrl) -> {
//            try {
//                if (shallInitSinkTable) {
//                    /**
//                     * 需要先初始化表MySQL目标库中的表
//                     */
//                    dataXWriter.initWriterTable(targetTabName, Collections.singletonList(jdbcUrl));
//                }
//
//// FIXME 这里不能用 MySQLSelectedTab
//                sinkFuncRef.set(createSinkFunction(dbName, targetTabName, selectedTab, jdbcUrl, dsFactory, dataXWriter));
//
//            } catch (Throwable e) {
//                exceptionLoader.set(new Object[]{jdbcUrl, e});
//            }
//        });

        //Objects.requireNonNull(sinkFuncRef.get(), "sinkFunc can not be null");
        sinkFuncRef.setParallelism(this.parallelism);
        return sinkFuncRef;
    }

    @Override
    protected Object parseType(CMeta cm) {
        return cm.getType();
    }

//    @Override
//    protected CreateChunjunSinkFunctionResult createSinkFactory(String jdbcUrl, String targetTabName, BasicDataSourceFactory dsFactory
//            , BasicDataXRdbmsWriter dataXWriter, SyncConf syncConf) {
//        IStreamTableMeta tabMeta = this.getStreamTableMeta(targetTabName);
//        DataXKafkaWriter rocksWriter = (DataXKafkaWriter) dataXWriter;
//
//        final CreateChunjunSinkFunctionResult createSinkResult = createSinkFunctionResult(jdbcUrl, rocksWriter, dsFactory, targetTabName, syncConf, tabMeta, this);
//        return createSinkResult;
//    }

    @Override
    protected Class<? extends JdbcDialect> getJdbcDialectClass() {
        return null;
    }

    @Override
    protected JdbcOutputFormat createChunjunOutputFormat(DataSourceFactory dsFactory, JdbcConf jdbcConf) {
        return null;
    }

    @Override
    protected void initChunjunJdbcConf(JdbcConf jdbcConf) {

    }

    @Override
    public IStreamTableMeta getStreamTableMeta(String tableName) {

        // DataxProcessor dataXProcessor = DataxProcessor.load(null, this.dataXName);
        //  DataxWriter writer = DataxWriter.load(null, this.dataXName);// dataXProcessor.getWriter(null);
        DataxReader reader = DataxReader.load(null, this.dataXName);

        List<ISelectedTab> tabs = reader.getSelectedTabs();

        Optional<ISelectedTab> find = tabs.stream().filter((f) -> StringUtils.equals(f.getName(), tableName)).findFirst();
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
        public Descriptor<IncrSelectedTabExtend> getSelectedTableExtendDescriptor() {
            return null;
        }
    }
}
