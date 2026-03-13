package com.qlangtech.tis.plugins.incr.flink.cdc.pipeline;

import com.google.common.collect.Maps;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.async.message.client.consumer.impl.MQListenerFactory;
import com.qlangtech.tis.datax.IDataXNameAware;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.datax.TableAlias;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.extension.util.AbstractPropAssist;
import com.qlangtech.tis.extension.util.OverwriteProps;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.DefaultTab;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.incr.IncrStreamFactory;
import com.qlangtech.tis.plugin.incr.TISSinkFactory;
import com.qlangtech.tis.plugin.timezone.TISTimeZone;
import com.qlangtech.tis.plugins.incr.flink.cdc.FlinkCDCPropAssist;
import com.qlangtech.tis.realtime.BasicTISSinkFactory;
import com.qlangtech.tis.realtime.DTOSourceTagProcessFunction;
import com.qlangtech.tis.realtime.TabSinkFunc;
import com.qlangtech.tis.sql.parser.tuple.creator.AdapterStreamTemplateData;
import com.qlangtech.tis.sql.parser.tuple.creator.IStreamIncrGenerateStrategy;
import com.qlangtech.tis.util.HeteroEnum;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.pipeline.SchemaChangeBehavior;

import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * 支持flink-cdc pipeline sink
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-22 09:01
 **/
public abstract class PipelineFlinkCDCSinkFactory<WRITER extends DataxWriter> extends BasicTISSinkFactory<Event> implements IDataXNameAware,
        IStreamIncrGenerateStrategy {
    public static final String KEY_SCHEMA_BEHAVIOR = "schemaBehavior";
    @FormField(ordinal = 14, validate = {Validator.require})
    public TISTimeZone timeZone;

    @FormField(ordinal = 13, type = FormFieldType.ENUM, validate = {Validator.require})
    public String schemaBehavior;

    public static final String DISPLAY_NAME_FLINK_PIPELINE_SINK = "FlinkCDC-Pipeline-Sink-";

    public final ZoneId getTimeZone() {
        return Objects.requireNonNull(timeZone, "timeZone can not be null").getTimeZone();// ZoneId.of(this.timeZone);
    }

    public final SchemaChangeBehavior getSchemaChangeBehavior() {
        return SchemaChangeBehavior.valueOf(this.schemaBehavior);
    }

    @Override
    public final IStreamTemplateData decorateMergeData(IStreamTemplateData mergeData) {
        return new AdapterStreamTemplateData(mergeData) {
            @Override
            public List<TableAlias> getDumpTables() {
                return Collections.singletonList(DTOSourceTagProcessFunction.createAllMergeTableAlias());
            }
        };
    }

    @Override
    public final Map<IDataxProcessor.TableMap, TabSinkFunc<?, ?, Event>> createSinkFunction(IDataxProcessor dataxProcessor,
                                                                                            IFlinkColCreator flinkColCreator) {
        IDataxReader reader = dataxProcessor.getReader(null);
        List<ISelectedTab> tabs = reader.getSelectedTabs();
        Map<IDataxProcessor.TableMap, TabSinkFunc<?, ?, Event>> sinkFuncs = Maps.newHashMap();

        IncrStreamFactory streamFactory = IncrStreamFactory.getFactory(dataxProcessor.identityValue());
        MQListenerFactory sourceListenerFactory =
                HeteroEnum.getIncrSourceListenerFactory((dataxProcessor).getDataXName());
        IFlinkColCreator<FlinkCol> sourceFlinkColCreator = Objects.requireNonNull(sourceListenerFactory,
                "sourceListenerFactory").createFlinkColCreator(reader);

        TableAlias allMergeTableAlias = DTOSourceTagProcessFunction.createAllMergeTableAlias();
        IDataxProcessor.TableMap tableMap = new IDataxProcessor.TableMap(Optional.empty(),
                new DefaultTab(allMergeTableAlias.getFrom()));
        tableMap.setFrom(allMergeTableAlias.getFrom());
        tableMap.setTo(allMergeTableAlias.getTo());

        @SuppressWarnings("all")
        WRITER writer = (WRITER) Objects.requireNonNull(dataxProcessor.getWriter(null),
                dataxProcessor.identityValue() + " relevant Writer can not be null");

        sinkFuncs.put(tableMap
                , createPipelineEventSinkFunc(dataxProcessor, writer, tabs, sourceFlinkColCreator, streamFactory));

        return sinkFuncs;
    }

    protected abstract TabSinkFunc<?, ?, Event> createPipelineEventSinkFunc(IDataxProcessor dataxProcessor
            , WRITER writer, List<ISelectedTab> tabs, IFlinkColCreator<FlinkCol> sourceFlinkColCreator, IncrStreamFactory streamFactory);

    @Override
    public final boolean flinkCDCPipelineEnable() {
        return true;
    }

    protected static abstract class BasicPipelineSinkDescriptor extends TISSinkFactory.BaseSinkFunctionDescriptor {
        public final AbstractPropAssist.Options<TISSinkFactory, ConfigOption> opts;

        public BasicPipelineSinkDescriptor() {
            super();
            this.opts = FlinkCDCPropAssist.createOpts(this);
            OverwriteProps schemaBehaviorOverwrite = new OverwriteProps();
            schemaBehaviorOverwrite.setDftVal(SchemaChangeBehavior.IGNORE.name());

            this.opts.add(KEY_SCHEMA_BEHAVIOR, PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR,
                    schemaBehaviorOverwrite);
        }

        @Override
        public final String getDisplayName() {
            return DISPLAY_NAME_FLINK_PIPELINE_SINK + this.getTargetType().name();
        }

        @Override
        public final PluginVender getVender() {
            return PluginVender.FLINK_CDC;
        }
    }
}
