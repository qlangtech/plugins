package com.qlangtech.tis.plugins.incr.flink.cdc.pipeline;

import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.datax.IDataxProcessor.TableMap;
import com.qlangtech.tis.datax.SourceColMetaGetter;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.datax.transformer.UDFDefinition;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.IInitWriterTableExecutor;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.cdc.common.event.CreateTableEvent;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;
import org.apache.flink.cdc.common.schema.Schema;
import org.apache.flink.cdc.common.types.DataType;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-31 15:03
 **/
public class SchemaEmitterFunction extends ProcessFunction<Event, Event> {
    private Set<TableId> alreadySendCreateTableTables = new HashSet<>();
    private Map<String, CreateTableEvent> createTableEventCache = new HashMap<>();

    /**
     * @param sinkDBName           sink端DB名称
     * @param tabs
     * @param sourceColsMetaMapper
     */
    public SchemaEmitterFunction(Optional<String> sinkDBName, Map<String, TableId> tab2TableID
            , IInitWriterTableExecutor writerTabInitialization //
            , ICDCPipelineTableOptionsCreator optionsCreator //
            , List<ISelectedTab> tabs
            , Map<String /**tabName*/, Pair<List<FlinkCol>, List<UDFDefinition>>> sourceColsMetaMapper,
                                 SourceColMetaGetter sourceColMetaGetter) {
        Function<SelectedTab, Map<String, String>> tableOptsCreator = optionsCreator.createTabOpts();
        TableId tableId = null;
        //  EntityName entityName = null;
        for (ISelectedTab tab : tabs) {
            //    entityName = paimonWriter.parseEntity(tab);

            TableMap tableMap = new TableMap(Optional.of(writerTabInitialization.getAutoCreateTableCanNotBeNull()), tab);
            tableId = tab2TableID.get(tab.getName());// TableId.tableId(entityName.getDbName(), entityName.getTableName());
            if (tableId == null) {
                throw new IllegalStateException("table:" + tab.getName() //
                        + " relevant tableId can not be null,exist keys:" //
                        + String.join(",", tab2TableID.keySet()));
            }
            //                    ? TableId.tableId(entityName.getTableName())
            //                    : TableId.tableId(sinkDBName.get(), entityName.getTableName());
            createTableEventCache.put(tableId.getTableName(), new CreateTableEvent(tableId, parseDDL(tableId,
                    tableOptsCreator.apply((SelectedTab) tab), (SelectedTab) tab,
                    Objects.requireNonNull(sourceColsMetaMapper.get(tableId.getTableName()), "table:" + tableId + " "
                            + "relevant flinkCols can not be null").getKey(), (colName) -> {

                        ColumnMetaData colMeta = sourceColMetaGetter.getColMeta(tableMap, colName);
                        return colMeta != null ? colMeta.getComment() : null;
                    })));
        }
    }

    private static Schema parseDDL(TableId tableId, Map<String, String> tableOpts, SelectedTab tab,
                                   List<FlinkCol> cols, Function<String, String> sourceColCommentGetter) {
        // Table table = parseDdl(ddlStatement, tableId);

        //List<CMeta> columns = tab.getCols();
        Schema.Builder schemaBuilder = Schema.newBuilder();
        schemaBuilder.options(tableOpts);
        DataType dataType = null;
        FlinkCol column = null;
        for (int i = 0; i < cols.size(); i++) {
            column = cols.get(i);
            dataType = column.flinkCDCPipelineEventProcess.getDataType();
            // String colName = column.name();
            schemaBuilder.physicalColumn(column.name, dataType, sourceColCommentGetter.apply(column.name), null);
        }
        schemaBuilder.comment(null);

        List<String> primaryKey = tab.getPrimaryKeys();
        if (Objects.nonNull(primaryKey) && !primaryKey.isEmpty()) {
            schemaBuilder.primaryKey(primaryKey);
        }
        tab.getPartitionKeys().ifPresent(schemaBuilder::partitionKey);
        return schemaBuilder.build();
    }

    @Override
    public void processElement(Event e, ProcessFunction<Event, Event>.Context ctx, Collector<Event> out) throws Exception {
        DataChangeEvent event = (DataChangeEvent) e;
        TableId tableId = event.tableId();
        if (!alreadySendCreateTableTables.contains(tableId)) {
            out.collect(Objects.requireNonNull(createTableEventCache.get(tableId.getTableName()),
                    "tableId:" + tableId + " relevant createTableEvent can not be null"));
            alreadySendCreateTableTables.add(tableId);
        }

        out.collect(e);
    }
}
