package com.qlangtech.tis.plugins.incr.flink.cdc.pipeline;

import com.google.common.collect.Maps;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.datax.SourceColMetaGetter;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.plugin.datax.transformer.UDFDefinition;
import com.qlangtech.tis.plugin.ds.IInitWriterTableExecutor;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.realtime.FilterUpdateBeforeEvent.DTOFilter;
import com.qlangtech.tis.realtime.SelectedTableTransformerRules;
import com.qlangtech.tis.realtime.TabSinkFunc;
import com.qlangtech.tis.realtime.dto.DTOStream;
import com.qlangtech.tis.realtime.transfer.DTO;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.cdc.common.configuration.Configuration;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.factories.Factory.Context;
import org.apache.flink.cdc.common.pipeline.PipelineOptions;
import org.apache.flink.cdc.common.sink.DataSink;
import org.apache.flink.cdc.common.source.DataSource;
import org.apache.flink.cdc.common.source.EventSourceProvider;
import org.apache.flink.cdc.common.source.MetadataAccessor;
import org.apache.flink.cdc.composer.definition.PipelineDef;
import org.apache.flink.cdc.composer.definition.SinkDef;
import org.apache.flink.cdc.composer.definition.SourceDef;
import org.apache.flink.cdc.composer.flink.FlinkPipelineComposer;
import org.apache.flink.cdc.composer.flink.translator.DataSinkTranslator;
import org.apache.flink.cdc.runtime.typeutils.EventTypeInfo;
import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap;
import org.apache.flink.shaded.guava31.com.google.common.collect.ImmutableMap.Builder;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.qlangtech.tis.realtime.transfer.DTO.EventType.UPDATE_BEFORE;

/**
 * 支持flink-cdc pipeline event sink
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-22 10:15
 **/
public abstract class PipelineEventSinkFunc<WRITER extends DataxWriter> extends TabSinkFunc<Sink<Event>, Void, Event> {

    private static final Logger logger = LoggerFactory.getLogger(PipelineEventSinkFunc.class);
    protected final IDataxProcessor dataxProcessor;
    protected final WRITER writer;
    protected final IInitWriterTableExecutor writerTabInitialization;
    protected final ICDCPipelineTableOptionsCreator optionsCreator;
    protected final PipelineFlinkCDCSinkFactory pipelineSinkFactory;
    protected final List<ISelectedTab> tabs;

    protected final Map<String /**sink tabName 别名*/, Pair<List<FlinkCol>, List<UDFDefinition>>> sourceColsMetaMapper;
    protected final Optional<String> sinkDBName;
    protected final Map<String, TableId> tab2TableID;

    public PipelineEventSinkFunc(IDataxProcessor dataxProcessor, PipelineFlinkCDCSinkFactory pipelineSinkFactory //
            , Optional<String> sinkDBName //paimonWriter.catalog.getDBName()
            , List<ISelectedTab> tabs //
            , IFlinkColCreator<FlinkCol> sourceFlinkColCreator, Sink<Event> sinkFunction //
            , int sinkTaskParallelism) {
        super(sinkFunction, sinkTaskParallelism);
        this.dataxProcessor = dataxProcessor;
        this.writer = Objects.requireNonNull((WRITER) dataxProcessor.getWriter(null), "dataWriter "
                + "can not be null");
        if (!(this.writer instanceof IInitWriterTableExecutor)) {
            throw new IllegalStateException(this.writer.getClass().getName() + " must be type of " + IInitWriterTableExecutor.class.getSimpleName());
        }
        this.writerTabInitialization = (IInitWriterTableExecutor) this.writer;
        if (!(this.writer instanceof ICDCPipelineTableOptionsCreator)) {
            throw new IllegalStateException(this.writer.getClass().getName() + " must be type of " + ICDCPipelineTableOptionsCreator.class.getSimpleName());
        }
        this.optionsCreator = (ICDCPipelineTableOptionsCreator) this.writer;
        this.pipelineSinkFactory = Objects.requireNonNull(pipelineSinkFactory, "pipelineSinkFactory can not be null");
        this.tabs = Objects.requireNonNull(tabs, "tabs can not be null");
        this.sinkDBName = sinkDBName;// paimonWriter.catalog.getDBName();
        Builder<String /**sinkTabName 别名*/, Pair<List<FlinkCol>, List<UDFDefinition>>> sourceColsMetaMapperBuilder =
                ImmutableMap.builder();
        Optional<SelectedTableTransformerRules> transformerOpt = null;
        ImmutableMap.Builder<String, TableId> tab2TableIDBuilder = ImmutableMap.builder();
        EntityName targetEntityName = null;
        for (ISelectedTab selTab : this.tabs) {
            targetEntityName = writer.parseEntity(selTab);
            tab2TableIDBuilder.put(selTab.getName(), TableId.tableId(targetEntityName.getDbName(),
                    targetEntityName.getTableName()));
            transformerOpt = SelectedTableTransformerRules.createTransformerRules(dataxProcessor.identityValue(),
                    selTab, Objects.requireNonNull(sourceFlinkColCreator, "sourceFlinkColCreator can not be null"));

            sourceColsMetaMapperBuilder.put(targetEntityName.getTableName(), Pair.of(FlinkCol.createCols(null, selTab
                    , sourceFlinkColCreator, transformerOpt, (rules) -> rules.overwriteColsWithContextParams() //
                    // (rules) -> rules.transformerColsWithoutContextParamsFlinkCol()
            ), transformerOpt.map((t) -> t.getTransformerRules().getTransformerUDFs()).orElse(null)));
        }
        this.tab2TableID = tab2TableIDBuilder.build();
        sourceColsMetaMapper = sourceColsMetaMapperBuilder.build();
    }

    @Override
    protected DataStream<Event> streamMap(DTOStream sourceStream) {
        DataStream<Event> result = null;
        if (sourceStream.clazz == DTO.class) {
            TypeInformation<Event> outputType = new EventTypeInfo();
            // Optional<String> dbName = paimonWriter.catalog.getDBName();
            SingleOutputStreamOperator<Event> outputOperator = sourceStream.getStream().filter(new DTOFilter(true,
                            Collections.emptyList())).name("skip_" + UPDATE_BEFORE) //
                    .setParallelism(this.sinkTaskParallelism) //
                    .map(new DTO2FlinkPipelineEventMapper(sinkDBName, this.sourceColsMetaMapper, this.tab2TableID), outputType) //
                    .name(dataxProcessor.identityValue() + "_dto2Event").setParallelism(this.sinkTaskParallelism);

            IDataxReader reader = this.dataxProcessor.getReader(null);

            SourceColMetaGetter sourceColMetaGetter = writerTabInitialization.getAutoCreateTableCanNotBeNull().enabledColumnComment() ?
                    reader.createSourceColMetaGetter() : SourceColMetaGetter.getNone();
            result = outputOperator.process(new SchemaEmitterFunction(sinkDBName, writer, this.writerTabInitialization, this.optionsCreator, this.tabs,
                    this.sourceColsMetaMapper, sourceColMetaGetter), outputType).name("schema_emitter");
            // outputOperator;
        } else if (sourceStream.clazz == org.apache.flink.cdc.common.event.Event.class) {
            result = sourceStream.getStream();
        } else if (sourceStream.clazz == RowData.class) {
            // 当chunjun作为source时
            logger.info("create stream directly, source type is RowData");
            // result = sourceStream.getStream();
            throw new UnsupportedOperationException("dataSource is create by chunjun is disabled");
        }
        if (result == null) {
            throw new IllegalStateException("not illegal source Stream class:" + sourceStream.clazz);
        }

        return result;
    }


    /**
     * @param sourceStream
     * @return
     * @see org.apache.flink.cdc.composer.flink.FlinkPipelineComposer# translate
     */
    @Override
    protected Void addSinkToSource(DataStream<Event> sourceStream) {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        FlinkPipelineComposer flinkPipelineComposer = FlinkPipelineComposer.ofApplicationCluster(env);
        DataSinkTranslator sinkTranslator = new DataSinkTranslator();
        Context dataSinkContext = createDataSinkContext();
        DataSink dataSink = createDataSink(dataSinkContext);
        PipelineDef pipelineDef = this.createPipelineDef();
        // StreamExecutionEnvironment env, DataSource dataSource, DataSink dataSink, DataSinkTranslator
        // sinkTranslator, DataStream<Event> stream, PipelineDef pipelineDef
        DataSource dataSource = this.createDataSource();
        flinkPipelineComposer.translate(env, dataSource, dataSink, sinkTranslator, sourceStream, pipelineDef);


        // FIXME create from sinkFactory
        //        Configuration configuration = Configuration.fromMap(Maps.newHashMap());
        //        SinkDef sinkDef = new SinkDef(dataxProcessor.identityValue(), null, configuration);

        //  PaimonDataSinkFactory paimonDataSinkFactory = new PaimonDataSinkFactory();

        //        SchemaChangeBehavior schemaChangeBehavior =
        //                configuration.get(PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR);
        //
        //        SchemaOperatorTranslator schemaOperatorTranslator =
        //                new SchemaOperatorTranslator(
        //                        schemaChangeBehavior,
        //                        configuration.get(PipelineOptions.PIPELINE_SCHEMA_OPERATOR_UID),
        //                        configuration.get(PipelineOptions.PIPELINE_SCHEMA_OPERATOR_RPC_TIMEOUT),
        //                        configuration.get(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE));
        //
        //        OperatorIDGenerator schemaOperatorIDGenerator =
        //                new OperatorIDGenerator(schemaOperatorTranslator.getSchemaOperatorUid());

        //        OperatorID schemaOperatorID = schemaOperatorIDGenerator.generate();
        //        sinkTranslator.translate(sinkDef, sourceStream, dataSink, schemaOperatorID);
        return null;
    }

    private PipelineDef createPipelineDef() {
        Configuration configuration = Configuration.fromMap(Maps.newHashMap());
        configuration.set(PipelineOptions.PIPELINE_PARALLELISM, this.sinkTaskParallelism);
        configuration.set(PipelineOptions.PIPELINE_LOCAL_TIME_ZONE, this.pipelineSinkFactory.getTimeZone().getId());
        configuration.set(PipelineOptions.PIPELINE_SCHEMA_CHANGE_BEHAVIOR,
                this.pipelineSinkFactory.getSchemaChangeBehavior());
        SinkDef sinkDef = new SinkDef(dataxProcessor.identityValue(), null, configuration);
        SourceDef source = new SourceDef(null, null, null);
        // SourceDef source, SinkDef sink, List< RouteDef > routes, List< TransformDef > transforms, List< UdfDef >
        // udfs, Configuration config
        return new PipelineDef(source, sinkDef, Collections.emptyList(), Collections.emptyList(),
                Collections.emptyList(), configuration);
    }

    private DataSource createDataSource() {
        return new DataSource() {
            @Override
            public EventSourceProvider getEventSourceProvider() {
                return null;
            }

            @Override
            public MetadataAccessor getMetadataAccessor() {
                return null;
            }
        };
    }

    protected abstract DataSink createDataSink(Context context);

    public abstract Context createDataSinkContext();

}
