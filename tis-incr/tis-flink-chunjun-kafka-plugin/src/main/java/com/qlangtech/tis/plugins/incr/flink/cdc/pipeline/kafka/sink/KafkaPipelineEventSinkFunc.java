package com.qlangtech.tis.plugins.incr.flink.cdc.pipeline.kafka.sink;

import com.google.common.collect.Maps;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.plugin.ds.DBConfig;
import com.qlangtech.tis.plugin.ds.IDataSourceFactoryGetter;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugins.datax.kafka.writer.DataXKafkaWriter;
import com.qlangtech.tis.plugins.incr.flink.cdc.pipeline.PipelineEventSinkFunc;
import com.qlangtech.tis.plugins.incr.flink.cdc.pipeline.PipelineFlinkCDCSinkFactory;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.common.factories.FactoryHelper;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.connectors.kafka.json.ChangeLogJsonFormatFactory;
import org.apache.flink.cdc.connectors.kafka.json.JsonSerializationType;
import org.apache.flink.cdc.connectors.kafka.sink.KafkaDataSink;
import org.apache.flink.cdc.connectors.kafka.sink.KafkaDataSinkFactory;
import org.apache.flink.cdc.connectors.kafka.sink.KafkaDataSinkOptions;
import org.apache.flink.cdc.connectors.kafka.sink.KeyFormat;
import org.apache.flink.cdc.connectors.kafka.sink.KeySerializationFactory;
import org.apache.flink.cdc.connectors.kafka.sink.PartitionStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.time.ZoneId;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;

import static org.apache.flink.cdc.connectors.kafka.sink.KafkaDataSinkOptions.KEY_FORMAT;
import static org.apache.flink.cdc.connectors.kafka.sink.KafkaDataSinkOptions.PROPERTIES_PREFIX;
import static org.apache.flink.cdc.connectors.kafka.sink.KafkaDataSinkOptions.SINK_TABLE_ID_TO_TOPIC_MAPPING;

/**
 * <a href="https://nightlies.apache.org/flink/flink-cdc-docs-release-3.5/docs/connectors/pipeline-connectors/kafka/">...</a>
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/3/13
 * // @see tis-flink-pipeline-paimon-plugin:com.qlangtech.tis.plugins.incr.flink.pipeline.paimon.sink.PaimonPipelineEventSinkFunc
 * @see DataXKafkaWriter
 */
public class KafkaPipelineEventSinkFunc extends PipelineEventSinkFunc<DataXKafkaWriter> {

    /**
     *
     * @param dataxProcessor
     * @param pipelineSinkFactory
     * @param sinkDBName
     * @param tabs
     * @param sourceFlinkColCreator
     * @param sinkTaskParallelism
     */
    public KafkaPipelineEventSinkFunc(IDataxProcessor dataxProcessor
            , PipelineFlinkCDCSinkFactory pipelineSinkFactory //
            , Optional<String> sinkDBName //
            , List<ISelectedTab> tabs //
            , IFlinkColCreator<FlinkCol> sourceFlinkColCreator //
            , int sinkTaskParallelism) {
        super(dataxProcessor, pipelineSinkFactory //
                , sinkDBName, tabs, sourceFlinkColCreator, null, sinkTaskParallelism);
    }

    @Override
    protected EntityName getWriterEntityName(ISelectedTab selTab) {

        IDataxReader reader = this.dataxProcessor.getReader(null);
        if (reader instanceof IDataSourceFactoryGetter) {
            try {
                String[] databaseName = new String[1];
                DBConfig dbConfig = ((IDataSourceFactoryGetter) reader).getDataSourceFactory().getDbConfig();
                dbConfig.vistDbName(new DBConfig.IProcess() {
                    @Override
                    public boolean visit(DBConfig config, String jdbcUrl, String ip, String dbName) throws Exception {
                        databaseName[0] = dbName;
                        return true;
                    }
                });
                return EntityName.create(databaseName[0], selTab.getName());
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return super.getWriterEntityName(selTab);
    }

    /**
     * @param context
     * @return
     * @see org.apache.flink.cdc.connectors.kafka.sink.KafkaDataSinkFactory#createDataSink(Factory.Context)
     */
//    protected DataSink createDataSink(Factory.Context context) {
//         KafkaDataSinkFactory factory = new KafkaDataSinkFactory();
//        return factory.createDataSink(context);
//    }
    @Override
    public DataSink createDataSink(Factory.Context context) {
        KafkaPipelineSinkFactory pipelineSinkFactory = ((KafkaPipelineSinkFactory) this.pipelineSinkFactory);
        KeyFormat keyFormat = context.getFactoryConfiguration().get(KEY_FORMAT);
        JsonSerializationType jsonSerializationType =
                context.getFactoryConfiguration().get(KafkaDataSinkOptions.VALUE_FORMAT);
        KafkaDataSinkFactory factory = new KafkaDataSinkFactory();
        FactoryHelper helper = FactoryHelper.createFactoryHelper(factory, context);
        helper.validateExcept(
                PROPERTIES_PREFIX, keyFormat.toString(), jsonSerializationType.toString());

        DeliveryGuarantee deliveryGuarantee = pipelineSinkFactory.deliveryGuarantee();
        //   context.getFactoryConfiguration().get(KafkaDataSinkOptions.DELIVERY_GUARANTEE);
        ZoneId zoneId = ZoneId.systemDefault();
        if (!Objects.equals(
                context.getPipelineConfiguration().get(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE),
                PipelineOptions.PIPELINE_LOCAL_TIME_ZONE.defaultValue())) {
            zoneId =
                    ZoneId.of(
                            context.getPipelineConfiguration()
                                    .get(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE));
        }
        SerializationSchema<Event> keySerialization =
                KeySerializationFactory.createSerializationSchema(
                        helper.getFormatConfig(keyFormat.toString()), keyFormat, zoneId);
        org.apache.flink.configuration.Configuration formatCfg = pipelineSinkFactory.format.createFlinkCfg();

        SerializationSchema<Event> valueSerialization =
                ChangeLogJsonFormatFactory.createSerializationSchema(
                        formatCfg,   // helper.getFormatConfig(jsonSerializationType.toString()),
                        jsonSerializationType,
                        zoneId);
        final Properties kafkaProperties = new Properties();
        Map<String, String> allOptions = context.getFactoryConfiguration().toMap();
        allOptions.keySet().stream()
                .filter(key -> key.startsWith(PROPERTIES_PREFIX))
                .forEach(
                        key -> {
                            final String value = allOptions.get(key);
                            final String subKey = key.substring((PROPERTIES_PREFIX).length());
                            kafkaProperties.put(subKey, value);
                        });
        String topic = context.getFactoryConfiguration().get(KafkaDataSinkOptions.TOPIC);
        boolean addTableToHeaderEnabled =
                context.getFactoryConfiguration()
                        .get(KafkaDataSinkOptions.SINK_ADD_TABLEID_TO_HEADER_ENABLED);
        String customHeaders =
                context.getFactoryConfiguration().get(KafkaDataSinkOptions.SINK_CUSTOM_HEADER);
        PartitionStrategy partitionStrategy = pipelineSinkFactory.getPartitionStrategy();
        // context.getFactoryConfiguration().get(KafkaDataSinkOptions.PARTITION_STRATEGY);
        String tableMapping = context.getFactoryConfiguration().get(SINK_TABLE_ID_TO_TOPIC_MAPPING);
        return new KafkaDataSink(
                deliveryGuarantee,
                kafkaProperties,
                partitionStrategy,
                zoneId,
                keySerialization,
                valueSerialization,
                topic,
                addTableToHeaderEnabled,
                customHeaders,
                tableMapping);
    }

    @Override
    public Factory.Context createDataSinkContext() {
        ImmutableMap.Builder<String, String> factoryParamBuilder = ImmutableMap.<String, String>builder();

        // 1. topic from DataXKafkaWriter
        factoryParamBuilder.put(KafkaDataSinkOptions.TOPIC.key(), this.writer.topic);

        // 2. value format from KafkaPipelineSinkFactory.format
        KafkaPipelineSinkFactory sinkFactory = (KafkaPipelineSinkFactory) this.pipelineSinkFactory;
        factoryParamBuilder.put(KafkaDataSinkOptions.VALUE_FORMAT.key(),
                Objects.requireNonNull(sinkFactory.format, "format can not be null").getDescriptor().getDisplayName());

        // 3. kafka producer properties with "properties." prefix from DataXKafkaWriter
        Map<String, Object> kafkaConfig = this.writer.buildKafkaConfig();
        kafkaConfig = Maps.newHashMap(kafkaConfig);
//        kafkaConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG,);
//        kafkaConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,);

        kafkaConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG
                , "org.apache.flink.kafka.shaded.org.apache.kafka.common.serialization.ByteArraySerializer");//   .StringSerializer.class.getName())
        kafkaConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG
                , "org.apache.flink.kafka.shaded.org.apache.kafka.common.serialization.ByteArraySerializer");//

        for (Map.Entry<String, Object> entry : kafkaConfig.entrySet()) {
//            if (ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG.equals(entry.getKey())
//                    || ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG.equals(entry.getKey())) {
//                continue;
//            }
            factoryParamBuilder.put(KafkaDataSinkOptions.PROPERTIES_PREFIX + entry.getKey(),
                    String.valueOf(entry.getValue()));
        }

        Configuration factoryConfiguration = Configuration.fromMap(factoryParamBuilder.build());

        // 4. pipeline configuration with timezone
        ImmutableMap.Builder<String, String> pipelineParamBuilder = ImmutableMap.<String, String>builder();
        pipelineParamBuilder.put(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE.key(),
                this.pipelineSinkFactory.getTimeZone().getId());
        Configuration pipelineConfiguration = Configuration.fromMap(pipelineParamBuilder.build());

        ClassLoader classLoader = KafkaPipelineEventSinkFunc.class.getClassLoader();
        return new FactoryHelper.DefaultContext(factoryConfiguration, pipelineConfiguration, classLoader);
    }


}
