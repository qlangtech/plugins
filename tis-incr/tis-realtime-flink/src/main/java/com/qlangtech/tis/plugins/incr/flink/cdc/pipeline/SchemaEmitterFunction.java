package com.qlangtech.tis.plugins.incr.flink.cdc.pipeline;

import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.datax.IDataxProcessor.TableMap;
import com.qlangtech.tis.datax.SourceColMetaGetter;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.datax.transformer.UDFDefinition;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.IInitWriterTableExecutor;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
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
    // private Map<String, String> tableOpts;
    // private List<ISelectedTab> tabs;

    /**
     * @param sinkDBName           sink端DB名称
     * @param tabs
     * @param sourceColsMetaMapper
     */
    public SchemaEmitterFunction(Optional<String> sinkDBName, DataxWriter paimonWriter
            , IInitWriterTableExecutor writerTabInitialization //
            , ICDCPipelineTableOptionsCreator optionsCreator //
            , List<ISelectedTab> tabs
            , Map<String /**tabName*/, Pair<List<FlinkCol>, List<UDFDefinition>>> sourceColsMetaMapper,
                                 SourceColMetaGetter sourceColMetaGetter) {
        Function<SelectedTab, Map<String, String>> tableOptsCreator = optionsCreator.createTabOpts();
        TableId tableId = null;
        EntityName entityName = null;
        for (ISelectedTab tab : tabs) {


            entityName = paimonWriter.parseEntity(tab);
            //            Objects.requireNonNull(sinkDBName, "sinkDBName can not be null").isEmpty()
            //                    ? tab.getEntityName() : EntityName.create(sinkDBName.get(), tab.getName());
            //            paimonWriter.autoCreateTable.appendTabPrefix()
            TableMap tableMap = new TableMap(Optional.of(writerTabInitialization.getAutoCreateTableCanNotBeNull()), tab);
            tableId = TableId.tableId(entityName.getDbName(), entityName.getTableName());
            //                    ? TableId.tableId(entityName.getTableName())
            //                    : TableId.tableId(sinkDBName.get(), entityName.getTableName());
            createTableEventCache.put(tableId.getTableName(), new CreateTableEvent(tableId, parseDDL(tableId,
                    tableOptsCreator.apply((SelectedTab) tab), (SelectedTab) tab,
                    Objects.requireNonNull(sourceColsMetaMapper.get(tableId.getTableName()), "table:" + tableId + " "
                            + "relevant flinkCols can not be null").getKey(), (colName) -> {
                        //                                if (tableExist) {
                        //                                    // 如果表已经存在情况下不能返回表的列comment内存，不然后续比较已经存在的schema流程会出错
                        //                                    // 由于paimoin 在获取已经存在的schema 会从 hdfs中获取 col元数据内容如下：
                        //                                    /**
                        //                                     * <pre>
                        //                                     *    {
                        //                                     *     "id" : 2,
                        //                                     *     "name" : "simple_code",
                        //                                     *     "type" : "VARCHAR(10)",
                        //                                     *     "description" : "为全局单号的简化号（后n位）"
                        //                                     *   }
                        //                                     *   但是通过 @see SchemaManager#schema json 反序列化后会丢失列comment
                        //                                     ，导致checkSchemaForExternalTable的 ‘newSchema.rowType()
                        //                                     .equalsIgnoreFieldId(existsSchema.rowType())’ 比较失败
                        //                                     *      public TableSchema schema(long id) {
                        //                                     *         return TableSchema.fromPath(fileIO, toSchemaPath(id));
                        //                                     *     }
                        //                                     * </pre>
                        //                                     */
                        //
                        //                                    /**
                        //                                     * <pre>
                        //                                     *  at org.apache.paimon.schema.SchemaManager
                        //                                     .checkSchemaForExternalTable(SchemaManager.java:257)
                        //                                     ~[paimon-bundle-1.1.1.jar:1.1.1]
                        //                                     * 	at org.apache.paimon.schema.SchemaManager.createTable
                        //                                     (SchemaManager.java:210) ~[paimon-bundle-1.1.1.jar:1.1.1]
                        //                                     * 	at org.apache.paimon.hive.HiveCatalog
                        //                                     .lambda$createTableImpl$25(HiveCatalog.java:1010)
                        //                                     ~[paimon-bundle-1.1.1.jar:1.1.1]
                        //                                     * 	at org.apache.paimon.hive.HiveCatalog.runWithLock
                        //                                     (HiveCatalog.java:1579) ~[paimon-bundle-1.1.1.jar:1.1.1]
                        //                                     * 	at org.apache.paimon.hive.HiveCatalog.createTableImpl
                        //                                     (HiveCatalog.java:1006) ~[paimon-bundle-1.1.1.jar:1.1.1]
                        //                                     * 	at org.apache.paimon.catalog.AbstractCatalog.createTable
                        //                                     (AbstractCatalog.java:356) ~[paimon-bundle-1.1.1.jar:1.1.
                        //                                     * </pre>
                        //                                     */
                        //                                    return null;
                        //                                }
                        ColumnMetaData colMeta = sourceColMetaGetter.getColMeta(tableMap, colName);
                        return colMeta != null ? colMeta.getComment() : null;
                    })));
        }
        //        if (MapUtils.isEmpty(tableOpts)) {
        //            throw new IllegalArgumentException("tableOpts can not be empty");
        //        }
        //        this.tableOpts = tableOpts;
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
