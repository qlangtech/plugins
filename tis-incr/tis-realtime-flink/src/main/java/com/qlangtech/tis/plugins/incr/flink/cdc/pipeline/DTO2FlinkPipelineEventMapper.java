package com.qlangtech.tis.plugins.incr.flink.cdc.pipeline;


import com.google.common.collect.Maps;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.plugins.incr.flink.cdc.DTOConvertTo;
import com.qlangtech.tis.plugin.datax.transformer.UDFDefinition;
import com.qlangtech.tis.plugins.incr.flink.cdc.BasicFlinkDataMapper;
import com.qlangtech.tis.realtime.FilterUpdateBeforeEvent.DTOFilter;
import com.qlangtech.tis.realtime.transfer.DTO;
import com.qlangtech.tis.realtime.transfer.DTO.EventType;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.flink.cdc.common.data.RecordData;
import org.apache.flink.cdc.common.event.DataChangeEvent;
import org.apache.flink.cdc.common.event.Event;
import org.apache.flink.cdc.common.event.TableId;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static com.qlangtech.tis.realtime.transfer.DTO.EventType.UPDATE_BEFORE;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-05-22 11:31
 * @see com.qlangtech.tis.plugins.incr.flink.cdc.DTO2RowDataMapper
 **/
public class DTO2FlinkPipelineEventMapper extends BasicFlinkDataMapper<DataChangeEvent, Event> {
    private transient Map<String, TISBinaryRecordDataGenerator> tab2GenMapper;
    private final Map<String /**tabName*/, Pair<List<FlinkCol>, List<UDFDefinition>>> sourceColsMetaMapper;
    // private final String sinkDBName;
    private final Map<String, TableId> tab2TableID;

    public DTO2FlinkPipelineEventMapper(Optional<String> dbName
            , Map<String /**tabName*/, Pair<List<FlinkCol>, List<UDFDefinition>>> sourceColsMetaMapper
            , Map<String, TableId> tab2TableID) {
        super(DTOConvertTo.FlinkCDCPipelineEvent);
        this.sourceColsMetaMapper = Objects.requireNonNull(sourceColsMetaMapper, "sourceColsMetaMapper can not be null");
        // this.sinkDBName = dbName.orElse(null);
        this.tab2TableID = Objects.requireNonNull(tab2TableID, "tab2TableID can not be null");
    }

    @Override
    protected void fillRowVals(DTO dto, DataChangeEvent row) {

    }

    private TISBinaryRecordDataGenerator getRecordDataGenerator(TableId tableId) {
        TISBinaryRecordDataGenerator recordDataGenerator = null;
        if (tab2GenMapper == null) {
            synchronized (this) {
                if (tab2GenMapper == null) {
                    tab2GenMapper = Maps.newHashMap();
                }
            }
        }
        if ((recordDataGenerator = tab2GenMapper.get(tableId.getTableName())) == null) {
            Pair<List<FlinkCol>, List<UDFDefinition>> pair = sourceColsMetaMapper.get(tableId.getTableName());
            List<FlinkCol> cols = pair.getKey();
            if (CollectionUtils.isEmpty(cols)) {
                throw new IllegalStateException("tableId:" + tableId + " relevant flinkCols can not be null");
            }
            org.apache.flink.cdc.common.types.DataType[] dataTypes = new org.apache.flink.cdc.common.types.DataType[cols.size()];
            for (int idx = 0; idx < cols.size(); idx++) {
                FlinkCol flinkCol = cols.get(idx);
                dataTypes[idx] =
                        flinkCol.flinkCDCPipelineEventProcess.getDataType();
            }


            recordDataGenerator = new TISBinaryRecordDataGenerator(dataTypes
                    , cols
                    , Optional.ofNullable(pair.getRight()).filter((udfs) -> CollectionUtils.isNotEmpty(udfs)));
            tab2GenMapper.put(tableId.getTableName(), recordDataGenerator);
        }
        return recordDataGenerator;
    }

    @Override
    protected DataChangeEvent createRowData(DTO dto) {
        DataChangeEvent changeEvent = null;

        TableId tableId = tab2TableID.get(dto.getTableName());
        if (tableId == null) {
            throw new IllegalStateException("table:" + dto.getTableName()
                    + " relevant tableId can not be null,exist keys:" + String.join(",", tab2TableID.keySet()));
        }

//        TableId tableId = StringUtils.isEmpty(sinkDBName)
//                ? TableId.tableId(dto.getTableName())
//                : TableId.tableId(sinkDBName, dto.getTableName());
        Map<String, Object> beforeVals = dto.getBefore();
        Map<String, Object> afterVals = dto.getAfter();

//        final BinaryRecordDataGenerator recordDataGenerator = Objects.requireNonNull(
//                tab2GenMapper.get(tableId.getTableName())
//                , "table:" + tableId.getTableName() + " relevant recordDataGenerator can not be null");

        final TISBinaryRecordDataGenerator recordDataGenerator = this.getRecordDataGenerator(tableId);

        Pair<List<FlinkCol>, List<UDFDefinition>> pair = Objects.requireNonNull(sourceColsMetaMapper.get(tableId.getTableName())
                , "table:" + tableId.getTableName() + " relevant sourceColsMeta can not be null");
        List<FlinkCol> sourceColsMeta = pair.getKey();
        EventType event = dto.getEventType();

        RecordData before = null;
        RecordData after = null;

        switch (event) {
            case UPDATE_BEFORE:
                throw new UnsupportedOperationException(
                        "shall not get event type:" + UPDATE_BEFORE + " shall skip by using " + DTOFilter.class.getName() + " ahead");
            case UPDATE_AFTER: {
                before = createRecordData(recordDataGenerator, sourceColsMeta, beforeVals);
                after = createRecordData(recordDataGenerator, sourceColsMeta, afterVals);
                changeEvent = DataChangeEvent.updateEvent(tableId, before, after);
                break;
            }
            case DELETE:
                before = createRecordData(recordDataGenerator, sourceColsMeta, beforeVals);
                changeEvent = DataChangeEvent.deleteEvent(tableId, before);
                break;
            case ADD:
                after = createRecordData(recordDataGenerator, sourceColsMeta, afterVals);
                changeEvent = DataChangeEvent.insertEvent(tableId, after);
                break;
            default:
                throw new IllegalStateException("illegal event type:" + event);
        }

        return changeEvent;
    }

    private RecordData createRecordData(TISBinaryRecordDataGenerator recordDataGenerator
            , List<FlinkCol> sourceColsMeta, Map<String, Object> vals) {
        Object[] rowFields = new Object[sourceColsMeta.size()];
        FlinkCol flinkCol = null;
        Object val = null;
        for (int idx = 0; idx < sourceColsMeta.size(); idx++) {
            flinkCol = sourceColsMeta.get(idx);
            val = vals.get(flinkCol.name);
            if (val != null) {
                rowFields[idx] = this.dtoConvert2Type.processVal(flinkCol, val);// flinkCol.processVal(this.dtoConvert2Type, val);
            }
        }

        return recordDataGenerator.generate(rowFields);
    }
}
