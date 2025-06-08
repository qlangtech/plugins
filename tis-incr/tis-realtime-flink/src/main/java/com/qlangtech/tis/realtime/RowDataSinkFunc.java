/**
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.qlangtech.tis.realtime;

import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.datax.TableAlias;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugins.incr.flink.cdc.DTO2RowDataMapper;
import com.qlangtech.tis.plugins.incr.flink.cdc.impl.RowDataTransformerMapper;
import com.qlangtech.tis.realtime.dto.DTOStream;
import com.qlangtech.tis.realtime.transfer.DTO;
import com.qlangtech.tis.realtime.transfer.DTO.EventType;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.collections.CollectionUtils;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

/**
 * (RowData,DTO) -> RowData
 */
public final class RowDataSinkFunc extends AbstractTabSinkFuncV1<SinkFunction<RowData>, DataStreamSink<RowData>, RowData> {
    private static final Logger logger = LoggerFactory.getLogger(RowDataSinkFunc.class);


    public RowDataSinkFunc(TableAlias tab
            , SinkFunction<RowData> sinkFunction //
            , List<String> primaryKeys //
            , IPluginContext pluginContext
            , final ISelectedTab selectedTab //List<FlinkCol> sourceColsMeta
            , IFlinkColCreator<FlinkCol> sourceFlinkColCreator
            , List<FlinkCol> sinkColsMeta //
            , boolean supportUpset
            , List<EventType> filterRowKinds
            , int sinkTaskParallelism
            , Optional<SelectedTableTransformerRules> transformerOpt) {
        super(tab, primaryKeys, sinkFunction
                , FlinkCol.createSourceCols(pluginContext, selectedTab, sourceFlinkColCreator, transformerOpt)
                , sinkColsMeta, sinkTaskParallelism
                , transformerOpt);

        //this.flinkColCreator = Objects.requireNonNull(flinkColCreator, "flinkColCreator can not be null");
        if (supportUpset || CollectionUtils.isNotEmpty(filterRowKinds)) {
            this.setSourceFilter(BasicTISSinkFactory.KEY_SKIP_UPDATE_BEFORE_EVENT
                    , new FilterUpdateBeforeEvent.RowDataFilter(supportUpset, filterRowKinds));
        }
    }


    public SinkFunctionProvider getSinkRuntimeProvider() {
        return SinkFunctionProvider.of(this.sinkFunction, this.sinkTaskParallelism);
    }

    @Override
    protected DataStream<RowData> streamMap(DTOStream sourceStream) {

        DataStream<RowData> result = null;
        if (sourceStream.clazz == DTO.class) {
            result = sourceStream.getStream().map(new DTO2RowDataMapper(this.sourceColsMeta))
                    .name(tab.getFrom() + "_dto2Rowdata")
                    .setParallelism(this.sinkTaskParallelism);
        } else if (sourceStream.clazz == RowData.class) {
            // 当chunjun作为source时
            logger.info("create stream directly, source type is RowData");
            result = sourceStream.getStream();
        }
        if (result == null) {
            throw new IllegalStateException("not illegal source Stream class:" + sourceStream.clazz);
        }

        if (transformers.isPresent()) {
            SelectedTableTransformerRules triple = transformers.get();
            List<FlinkCol> transformerColsWithoutContextParamsFlinkCol = triple.transformerColsWithoutContextParamsFlinkCol();
            LogicalType[] fieldDataTypes
                    = transformerColsWithoutContextParamsFlinkCol
                    .stream().map((colmeta) -> colmeta.type.getLogicalType()).toArray(LogicalType[]::new);

            String[] colNames = transformerColsWithoutContextParamsFlinkCol
                    .stream().map((colmeta) -> colmeta.name).toArray(String[]::new);
            TypeInformation<RowData> outputType = InternalTypeInfo.of(RowType.of(fieldDataTypes, colNames));
            // = (TypeInformation<RowData>) TypeConversions.fromDataTypeToLegacyInfo(TypeConversions.fromLogicalToDataType(rowType));

            logger.info("transformerColsWithoutContextParamsFlinkCol size:{},colNames:{}"
                    , transformerColsWithoutContextParamsFlinkCol.size(), String.join(",", colNames));
            return result.map(new RowDataTransformerMapper(triple, transformerColsWithoutContextParamsFlinkCol.size()), outputType)
                    .name(tab.getFrom() + "_transformer").setParallelism(this.sinkTaskParallelism);
        } else {
            return result;
        }
    }

    @Override
    protected DataStreamSink<RowData> addSinkToSource(DataStream<RowData> source) {
        return source.addSink(sinkFunction).name(tab.getTo()).setParallelism(this.sinkTaskParallelism);
    }


}
