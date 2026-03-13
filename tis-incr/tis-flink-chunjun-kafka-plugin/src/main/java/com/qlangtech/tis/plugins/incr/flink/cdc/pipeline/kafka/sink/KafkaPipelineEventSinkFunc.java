package com.qlangtech.tis.plugins.incr.flink.cdc.pipeline.kafka.sink;

import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugins.datax.kafka.writer.DataXKafkaWriter;
import com.qlangtech.tis.plugins.incr.flink.cdc.pipeline.PipelineEventSinkFunc;
import com.qlangtech.tis.plugins.incr.flink.cdc.pipeline.PipelineFlinkCDCSinkFactory;
import org.apache.flink.cdc.common.factories.Factory;
import org.apache.flink.cdc.common.sink.DataSink;

import java.time.ZoneId;
import java.util.List;
import java.util.Optional;

/**
 * <a href="https://nightlies.apache.org/flink/flink-cdc-docs-release-3.5/docs/connectors/pipeline-connectors/kafka/">...</a>
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
        super(dataxProcessor, pipelineSinkFactory, sinkDBName, tabs, sourceFlinkColCreator, null, sinkTaskParallelism);
    }

    /**
     * @param context
     * @return
     * @see org.apache.flink.cdc.connectors.kafka.sink.KafkaDataSink
     */
    @Override
    protected DataSink createDataSink(Factory.Context context) {
//        DeliveryGuarantee deliveryGuarantee,
//        Properties kafkaProperties,
//        PartitionStrategy partitionStrategy,
//        ZoneId zoneId,
//        SerializationSchema<Event> keySerialization,
//        SerializationSchema<Event> valueSerialization,
//        String topic,
//        boolean addTableToHeaderEnabled,
//        String customHeaders,
//        String tableMapping

        String topic = this.writer.topic;
        ZoneId zoneId = this.pipelineSinkFactory.getTimeZone();
//        return new KafkaDataSink(catalogOpts, tableOptions, commitUser, partitionMaps, serializer, zoneId,
//                schemaOperatorUid);
        return null;
    }

    @Override
    public Factory.Context createDataSinkContext() {
        return null;
    }


}
