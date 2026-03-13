package com.qlangtech.tis.plugins.incr.flink.cdc.pipeline.kafka.sink;

import com.google.common.collect.Sets;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.compiler.incr.ICompileAndPackage;
import com.qlangtech.tis.compiler.streamcode.CompileAndPackage;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.incr.IncrStreamFactory;
import com.qlangtech.tis.plugins.datax.kafka.writer.DataXKafkaWriter;
import com.qlangtech.tis.plugins.incr.flink.cdc.pipeline.PipelineFlinkCDCSinkFactory;
import com.qlangtech.tis.realtime.TabSinkFunc;
import org.apache.flink.cdc.common.event.Event;

import java.util.List;
import java.util.Optional;

/**
 * <a href="https://nightlies.apache.org/flink/flink-cdc-docs-release-3.5/docs/connectors/pipeline-connectors/kafka/">...</a>
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/3/13
 */
public class KafkaPipelineSinkFactory extends PipelineFlinkCDCSinkFactory<DataXKafkaWriter> {
    @Override
    public ICompileAndPackage getCompileAndPackageManager() {
        return new CompileAndPackage(Sets.newHashSet(KafkaPipelineSinkFactory.class));
    }

    @Override
    protected TabSinkFunc<?, ?, Event> createPipelineEventSinkFunc(IDataxProcessor dataxProcessor //
            , DataXKafkaWriter writer, List<ISelectedTab> tabs //
            , IFlinkColCreator<FlinkCol> sourceFlinkColCreator, IncrStreamFactory streamFactory) {
        return new KafkaPipelineEventSinkFunc(dataxProcessor, this //
                , Optional.empty(), tabs, sourceFlinkColCreator,
                streamFactory.getParallelism());
    }

    @TISExtension
    public static class DftDesc extends BasicPipelineSinkDescriptor {
        public DftDesc() {
            super();
        }

        @Override
        protected IEndTypeGetter.EndType getTargetType() {
            return EndType.Kafka;
        }
    }
}
