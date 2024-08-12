/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qlangtech.tis.realtime;

import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.TableAlias;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.incr.TISSinkFactory;
import com.qlangtech.tis.plugins.incr.flink.cdc.DTO2RowDataMapper;
import com.qlangtech.tis.plugins.incr.flink.cdc.impl.RowDataTransformerMapper;
import com.qlangtech.tis.realtime.dto.DTOStream;
import com.qlangtech.tis.realtime.transfer.DTO;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-05-13 23:01
 **/
public abstract class BasicTISSinkFactory<TRANSFER_OBJ> extends TISSinkFactory {
    private static final Logger logger = LoggerFactory.getLogger(BasicTISSinkFactory.class);

    @Override
    public abstract Map<TableAlias, TabSinkFunc<TRANSFER_OBJ>> createSinkFunction(IDataxProcessor dataxProcessor, IFlinkColCreator flinkColCreator);


    /**
     * (RowData,DTO) -> DTO
     */
    public final static class DTOSinkFunc extends TabSinkFunc<DTO> {

        /**
         * @param tab
         * @param sinkFunction
         * @param supportUpset 是否支持类似MySQL的replace类型的更新操作？
         */
        public DTOSinkFunc(TableAlias tab, List<String> primaryKeys, SinkFunction<DTO> sinkFunction
                , boolean supportUpset, List<FlinkCol> colsMeta, int sinkTaskParallelism) {
            super(tab, primaryKeys, sinkFunction, colsMeta, sinkTaskParallelism);
            if (supportUpset) {
                this.setSourceFilter("skipUpdateBeforeEvent", new FilterUpdateBeforeEvent.DTOFilter());
            }
        }

        @Override
        protected DataStream<DTO> streamMap(DTOStream sourceStream) {
            if (sourceStream.clazz == DTO.class) {
                return sourceStream.getStream();
            } else if (sourceStream.clazz == RowData.class) {
                throw new UnsupportedOperationException("RowData -> DTO is not support");
            }

            throw new IllegalStateException("not illegal source Stream class:" + sourceStream.clazz);
        }
    }

    /**
     * (RowData,DTO) -> RowData
     */
    public final static class RowDataSinkFunc extends TabSinkFunc<RowData> {

        public static Optional<SelectedTableTransformerRules>
        createTransformerRules(String dataXName, TableAlias tabAlias, ISelectedTab tab, IFlinkColCreator<FlinkCol> sourceFlinkColCreator) {
            final IPluginContext dataXContext = IPluginContext.namedContext(dataXName);
            RecordTransformerRules transformerRules = RecordTransformerRules.loadTransformerRules(dataXContext, tabAlias.getFrom());
            Optional<SelectedTableTransformerRules> transformerOpt
                    = (transformerRules != null)
                    ? Optional.of(new SelectedTableTransformerRules(transformerRules, tab, sourceFlinkColCreator, dataXContext))
                    : Optional.empty();
            return transformerOpt;
        }
        //  private final IFlinkColCreator<FlinkCol> flinkColCreator;

//        public RowDataSinkFunc(TableAlias tab
//                , SinkFunction<RowData> sinkFunction, List<String> primaryKeys, List<FlinkCol> colsMeta
//                , boolean supportUpset, int sinkTaskParallelism) {
//            this(tab, sinkFunction, primaryKeys, colsMeta, colsMeta, supportUpset, sinkTaskParallelism, Optional.empty());
//        }

        private static List<FlinkCol> createSourceCols(IPluginContext pluginContext
                , final ISelectedTab tab, IFlinkColCreator<FlinkCol> sourceFlinkColCreator, Optional<SelectedTableTransformerRules> transformerOpt) {
            List<FlinkCol> sourceColsMeta = null;
            if (transformerOpt.isPresent()) {
                SelectedTableTransformerRules rules = transformerOpt.get();
//                ITransformerBuildInfo transformerBuildInfo = rules.getTransformerRules().createTransformerBuildInfo(pluginContext);
//                transformerBuildInfo.overwriteColsWithContextParams(tab.getCols());
                sourceColsMeta = rules.originColsWithContextParamsFlinkCol(); // FlinkCol.getAllTabColsMeta(transformerBuildInfo.originColsWithContextParams(), sourceFlinkColCreator);
            } else {
                sourceColsMeta = FlinkCol.getAllTabColsMeta(tab.getCols(), sourceFlinkColCreator);
            }

            return sourceColsMeta;
        }

        public RowDataSinkFunc(TableAlias tab
                , SinkFunction<RowData> sinkFunction //
                , List<String> primaryKeys //
                , IPluginContext pluginContext
                , final ISelectedTab selectedTab //List<FlinkCol> sourceColsMeta
                , IFlinkColCreator<FlinkCol> sourceFlinkColCreator
                , List<FlinkCol> sinkColsMeta //
                , boolean supportUpset, int sinkTaskParallelism
                , Optional<SelectedTableTransformerRules> transformerOpt) {
            super(tab, primaryKeys, sinkFunction, createSourceCols(pluginContext, selectedTab, sourceFlinkColCreator, transformerOpt), sinkColsMeta, sinkTaskParallelism
                    , transformerOpt);

            //this.flinkColCreator = Objects.requireNonNull(flinkColCreator, "flinkColCreator can not be null");
            if (supportUpset) {
                this.setSourceFilter("skipUpdateBeforeEvent"
                        , new FilterUpdateBeforeEvent.RowDataFilter());
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
                logger.info("create stream directly, source type is RowData");
                result = sourceStream.getStream();
            }
            if (result == null) {
                throw new IllegalStateException("not illegal source Stream class:" + sourceStream.clazz);
            }

            if (transformers.isPresent()) {
                SelectedTableTransformerRules triple = transformers.get();
                // RecordTransformerRules rule = triple.getTransformerRules();
//                List<IColMetaGetter> cols = triple.overwriteColsWithContextParams();
//                triple.originColsWithContextParamsFlinkCol();
                return result.map(new RowDataTransformerMapper(triple))
                        .name(tab.getFrom() + "_transformer").setParallelism(this.sinkTaskParallelism);
            } else {
                return result;
            }
        }
    }
}
