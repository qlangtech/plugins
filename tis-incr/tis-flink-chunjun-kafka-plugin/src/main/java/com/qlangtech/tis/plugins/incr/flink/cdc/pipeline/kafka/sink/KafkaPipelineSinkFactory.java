package com.qlangtech.tis.plugins.incr.flink.cdc.pipeline.kafka.sink;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.compiler.incr.ICompileAndPackage;
import com.qlangtech.tis.compiler.streamcode.CompileAndPackage;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.util.OverwriteProps;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.incr.IncrStreamFactory;
import com.qlangtech.tis.plugins.datax.kafka.writer.DataXKafkaWriter;
import com.qlangtech.tis.plugins.incr.flink.cdc.pipeline.PipelineFlinkCDCSinkFactory;
import com.qlangtech.tis.plugins.incr.flink.chunjun.kafka.format.FormatFactory;
import com.qlangtech.tis.realtime.TabSinkFunc;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.connectors.kafka.sink.KafkaDataSinkOptions;
import org.apache.flink.cdc.connectors.kafka.sink.PartitionStrategy;
import org.apache.flink.connector.base.DeliveryGuarantee;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * <a href="https://nightlies.apache.org/flink/flink-cdc-docs-release-3.5/docs/connectors/pipeline-connectors/kafka/">...</a>
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/3/13
 */
public class KafkaPipelineSinkFactory extends PipelineFlinkCDCSinkFactory<DataXKafkaWriter> {
    @FormField(ordinal = 0, validate = {Validator.require})
    public FormatFactory format;

    @FormField(ordinal = 2, type = FormFieldType.ENUM, validate = {Validator.require})
    public String semantic;

    //PartitionStrategy
    @FormField(ordinal = 3, type = FormFieldType.ENUM, validate = {Validator.require})
    public String partitionStrategy;

    public DeliveryGuarantee deliveryGuarantee() {
        for (DeliveryGuarantee guarantee : DeliveryGuarantee.values()) {
            if (StringUtils.equals(guarantee.toString(), semantic)) {
                return guarantee;
            }
        }
        throw new IllegalStateException("illegal semantic:" + this.semantic);
    }

    public PartitionStrategy getPartitionStrategy() {
        for (PartitionStrategy ps : PartitionStrategy.values()) {
            if (StringUtils.equalsIgnoreCase(this.partitionStrategy, (ps.name()))) {
                return ps;
            }
        }
        throw new IllegalStateException("illegal strategy:" + this.partitionStrategy);
    }


    public static List<Option> getSupportSemantic() {
        List<Option> semantic = Lists.newArrayList();
        for (DeliveryGuarantee dg : DeliveryGuarantee.values()) {
            semantic.add(new Option((dg.name()), dg.toString()));
        }
        return semantic;
        // return Lists.newArrayList(new Option("Exactly-Once", "exactly-once"), new Option("At-Least-Once", "at-least-once"));
    }

    public static List<? extends Descriptor> supportedFormats(List<? extends Descriptor> descs) {
        if (CollectionUtils.isEmpty(descs)) {
            return Collections.emptyList();
        }
        return descs.stream().filter((desc) ->
                ((FormatFactory.BasicFormatDescriptor) desc).getEndType().sinkSupport).collect(Collectors.toList());
    }

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
            ConfigOption<DeliveryGuarantee> dg = KafkaDataSinkOptions.DELIVERY_GUARANTEE;
            opts.add("semantic", dg, new OverwriteProps().setEnumOpts(getSupportSemantic()));
            opts.add("partitionStrategy" //
                    , KafkaDataSinkOptions.PARTITION_STRATEGY //
                    , new OverwriteProps().setLabelRewrite((l) -> "分区策略"));
        }


        @Override
        protected IEndTypeGetter.EndType getTargetType() {
            return EndType.Kafka;
        }
    }
}
