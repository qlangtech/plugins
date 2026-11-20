package com.qlangtech.tis.plugins.incr.flink.cdc.pipeline.kafka.sink;

import com.qlangtech.plugins.incr.flink.chunjun.doris.sink.TestFlinkSinkExecutor;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.incr.IncrStreamFactory;
import com.qlangtech.tis.plugin.timezone.TISTimeZone;
import com.qlangtech.tis.plugins.incr.flink.chunjun.kafka.format.canaljson.TISCanalJsonFormatFactory;
import com.qlangtech.tis.plugins.incr.flink.chunjun.kafka.sink.TestChujunKafkaSinkFactoryIntegration;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;
import org.junit.BeforeClass;
import org.junit.Test;
import org.mockito.Mockito;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/3/14
 */
public class TestKafkaPipelineSinkFactory extends TestFlinkSinkExecutor<KafkaPipelineSinkFactory, Event> {

    @BeforeClass
    public static void initializeKafka() throws Exception {
        TestChujunKafkaSinkFactoryIntegration.initializeKafka();
    }

    @Test
    public void testKafkaWriter() throws Exception {
        IncrStreamFactory streamFactory = mock("streamFactory", IncrStreamFactory.class);
        //Mockito.when(streamFactory.supportRateLimiter()).thenReturn(false);
        Mockito.when(streamFactory.getParallelism()).thenReturn(1);
        IncrStreamFactory.stubStreamFactory = (dataXName) -> {
            return streamFactory;
        };
        super.testSinkSync();
    }

    @Override
    protected BasicDataSourceFactory getDsFactory() {
        return null;
    }

    @Override
    protected KafkaPipelineSinkFactory getSinkFactory() {
        KafkaPipelineSinkFactory sinkFactory = new KafkaPipelineSinkFactory();
        sinkFactory.timeZone = TISTimeZone.dftZone();
        sinkFactory.schemaBehavior = SchemaChangeBehavior.IGNORE.name();
        sinkFactory.format = new TISCanalJsonFormatFactory();
        return sinkFactory;
    }

    @Override
    protected DataxWriter createDataXWriter() {
        return TestChujunKafkaSinkFactoryIntegration.createDataXKafkaWriter();
    }

    @Override
    protected boolean isFlinkCDCPipelineEnable() {
        return true;
    }
}
