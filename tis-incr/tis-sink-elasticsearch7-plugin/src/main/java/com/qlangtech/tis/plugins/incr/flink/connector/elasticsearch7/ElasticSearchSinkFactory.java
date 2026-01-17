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

package com.qlangtech.tis.plugins.incr.flink.connector.elasticsearch7;


import com.alibaba.citrus.turbine.Context;
import com.alibaba.datax.plugin.writer.elasticsearchwriter.ESColumn;
import com.google.common.collect.Sets;
import com.qlangtech.org.apache.http.HttpHost;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.async.message.client.consumer.impl.MQListenerFactory;
import com.qlangtech.tis.compiler.incr.ICompileAndPackage;
import com.qlangtech.tis.compiler.streamcode.CompileAndPackage;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.datax.impl.ESTableAlias;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.manage.IAppSource;
import com.qlangtech.tis.plugin.AuthToken;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.aliyun.NoneToken;
import com.qlangtech.tis.plugin.aliyun.UsernamePassword;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.DataXElasticsearchWriter;
import com.qlangtech.tis.plugin.datax.elastic.ElasticEndpoint;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugins.incr.flink.cdc.AbstractRowDataMapper;
import com.qlangtech.tis.realtime.BasicTISSinkFactory;
import com.qlangtech.tis.realtime.RowDataSinkFunc;
import com.qlangtech.tis.realtime.SelectedTableTransformerRules;
import com.qlangtech.tis.realtime.TabSinkFunc;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.util.HeteroEnum;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.delete.DeleteRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;


/**
 * https://ci.apache.org/projects/flink/flink-docs-master/zh/docs/connectors/datastream/elasticsearch/
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-09-28 19:45
 **/
@Public
public class ElasticSearchSinkFactory extends BasicTISSinkFactory<RowData> {
    public static final String DISPLAY_NAME_FLINK_CDC_SINK = "Flink-ElasticSearch-Sink";
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchSinkFactory.class);
    private static final int DEFAULT_PARALLELISM = 1;// parallelism
    // bulk.flush.max.actions
    @FormField(ordinal = 1, advance = true, type = FormFieldType.INT_NUMBER, validate = Validator.integer)
    public Integer bulkFlushMaxActions;

    @FormField(ordinal = 2, advance = true, type = FormFieldType.INT_NUMBER, validate = Validator.integer)
    public Integer bulkFlushMaxSizeMb;

    @FormField(ordinal = 0, type = FormFieldType.INT_NUMBER, validate = {Validator.integer, Validator.require})
    public Integer bulkFlushIntervalMs;

    static {
        TISXContentBuilderExtension.load();
    }


    @Override
    public Map<IDataxProcessor.TableMap, TabSinkFunc<?, ?, RowData>> createSinkFunction(IDataxProcessor dataxProcessor, IFlinkColCreator sourceFlinkColCreator) {

        DataXElasticsearchWriter dataXWriter = (DataXElasticsearchWriter) dataxProcessor.getWriter(null);
        MQListenerFactory sourceListener = HeteroEnum.getIncrSourceListenerFactory(((IAppSource) dataxProcessor).getDataXName());

        Objects.requireNonNull(dataXWriter, "dataXWriter can not be null");
        ElasticEndpoint token = dataXWriter.getToken();

        ESTableAlias esSchema = DataXElasticsearchWriter.getEsTableAlias(dataxProcessor);
//        Optional<IDataxProcessor.TableMap> first = dataxProcessor.getFirstTableMap(null);// dataxProcessor.getTabAlias(null, false).findFirst();
//        if (first.isPresent()) {
//            IDataxProcessor.TableMap value = first.get();
//            if (!(value instanceof ESTableAlias)) {
//                throw new IllegalStateException("value must be type of 'ESTableAlias',but now is :" + value.getClass());
//            }
//            esSchema = (ESTableAlias) value;
//        }

        Objects.requireNonNull(esSchema, "esSchema can not be null");
//        List<CMeta> cols = esSchema.getSourceCols();
//        if (CollectionUtils.isEmpty(cols)) {
//            throw new IllegalStateException("cols can not be null");
//        }
//        Optional<CMeta> firstPK = cols.stream().filter((c) -> c.isPk()).findFirst();
//        if (!firstPK.isPresent()) {
//            throw new IllegalStateException("has not set PK col");
//        }

        /********************************************************
         * 初始化索引Schema
         *******************************************************/
        List<ESColumn> esCols = dataXWriter.initialIndex(dataxProcessor);
        List<HttpHost> transportAddresses = new ArrayList<>();
        String endpoint = token.getEndpoint();
        if (StringUtils.isEmpty(endpoint)) {
            throw new IllegalStateException("param endpoint can not be empty");
        }
        transportAddresses.add(HttpHost.create(endpoint));

        // ISelectedTab tab = null;
        IDataxReader reader = dataxProcessor.getReader(null);

        List<IColMetaGetter> sinkMcols = Lists.newArrayList();
        List<String> primaryKeys = Lists.newArrayList();

        for (ESColumn col : esCols) {
            if (col.isPk()) {
                primaryKeys.add(col.getName());
            }
            sinkMcols.add(IColMetaGetter.create(col.getName(), col.getEsType().getDataType(), col.isPk()));
        }

        ISelectedTab tab = null;
        for (ISelectedTab selectedTab : reader.getSelectedTabs()) {
            tab = selectedTab;
            break;
        }

        IFlinkColCreator<FlinkCol> flinkColCreator = AbstractRowDataMapper::mapFlinkCol;

        Objects.requireNonNull(tab, "tab ca not be null");

        final List<FlinkCol> sinkColsMeta = FlinkCol.getAllTabColsMeta(sinkMcols, flinkColCreator);
        ElasticsearchSink.Builder<RowData> sinkBuilder
                = new ElasticsearchSink.Builder<>(transportAddresses
                , new DefaultElasticsearchSinkFunction(
                esCols.stream().map((c) -> c.getName()).collect(Collectors.toSet())
                , sinkColsMeta
                //, firstPK.get().getName()
                , primaryKeys.get(0)
                , dataXWriter.getIndexName()));

        if (this.bulkFlushMaxActions != null) {
            sinkBuilder.setBulkFlushMaxActions(this.bulkFlushMaxActions);
        }

        if (this.bulkFlushMaxSizeMb != null) {
            sinkBuilder.setBulkFlushMaxSizeMb(bulkFlushMaxSizeMb);
        }

        if (this.bulkFlushIntervalMs != null) {
            sinkBuilder.setBulkFlushInterval(this.bulkFlushIntervalMs);
        }

        sinkBuilder.setFailureHandler(new DefaultActionRequestFailureHandler());

        token.accept(new AuthToken.Visitor<Void>() {
            @Override
            public Void visit(NoneToken noneToken) {
                return null;
            }

            @Override
            public Void visit(UsernamePassword accessKey) {
                sinkBuilder.setRestClientFactory(new TISElasticRestClientFactory(accessKey.userName, accessKey.password));
                return null;
            }
        });
        // final List<FlinkCol> sourceColsMeta = FlinkCol.getAllTabColsMeta(tab.getCols(), sourceFlinkColCreator);

        if (!StringUtils.equals(esSchema.getName(), tab.getName())) {
            throw new IllegalStateException("esSchema.getFrom():" + esSchema.getName() + " must be equal with tab.getName():" + tab.getName());
        }
        Optional<SelectedTableTransformerRules> transformerOpt
                = SelectedTableTransformerRules.createTransformerRules(dataxProcessor.identityValue() //, esSchema
                , tab, sourceFlinkColCreator);

        final IDataxProcessor.TableMap esTabMap = new IDataxProcessor.TableMap(Optional.empty(), esSchema);
        return Collections.singletonMap(esTabMap
                , new RowDataSinkFunc(esTabMap, sinkBuilder.build(), primaryKeys
                        , IPluginContext.namedContext(dataxProcessor.identityValue())
                        , tab
                        , sourceFlinkColCreator
                        //, sourceColsMeta
                        , sinkColsMeta
                        , true, sourceListener.getFilterRowKinds(), DEFAULT_PARALLELISM, transformerOpt));
    }


    private static class DefaultActionRequestFailureHandler implements ActionRequestFailureHandler, Serializable {
        @Override
        public void onFailure(ActionRequest actionRequest, Throwable throwable, int restStatusCode, RequestIndexer requestIndexer) throws Throwable {
            //throwable.printStackTrace();
            logger.error(throwable.getMessage(), throwable);
        }
    }

    @Override
    public ICompileAndPackage getCompileAndPackageManager() {
        return new CompileAndPackage(Sets.newHashSet(ElasticSearchSinkFactory.class));
    }

    private static class DefaultElasticsearchSinkFunction implements ElasticsearchSinkFunction<RowData>, Serializable {
        private final Set<String> cols;
        private final String pkName;
        private final String targetIndexName;
        private final List<FlinkCol> vgetters;

        public DefaultElasticsearchSinkFunction(Set<String> cols
                , List<FlinkCol> vgetters, String pkName, String targetIndexName) {
            this.vgetters = vgetters;
            this.cols = cols;
            this.pkName = pkName;
            this.targetIndexName = targetIndexName;
            if (StringUtils.isEmpty(targetIndexName)) {
                throw new IllegalArgumentException("param targetIndexName can not be null");
            }
        }

        private IndexRequest createIndexRequest(RowData element) {
            Map<String, Object> json = getESVals(element);
            Object pkVal = json.get(this.pkName);
            IndexRequest request = Requests.indexRequest()
                    .index(this.targetIndexName)
                    //.type("my-type")
                    .source(json);
            if (pkVal != null) {
                request.id(String.valueOf(pkVal));
            }
            return request;
        }

        private Map<String, Object> getESVals(RowData element) {
            Map<String, Object> json = new HashMap<>();

            Object val = null;
            for (FlinkCol get : vgetters) {
                if (!cols.contains(get.name)) {
                    continue;
                }
                val = get.getRowDataVal(element);
                if (val instanceof DecimalData) {
                    val = ((DecimalData) val).toBigDecimal();
                } else if (val instanceof StringData) {
                    val = ((StringData) val).toString();
                }
                json.put(get.name, val);
            }
            return json;
        }

        /**
         * // @see org.apache.flink.streaming.connectors.elasticsearch.table.RowElasticsearchSinkFunction
         *
         * @param element
         * @param ctx
         * @param indexer
         */
        @Override
        public void process(RowData element, RuntimeContext ctx, RequestIndexer indexer) {
            switch (element.getRowKind()) {
                case INSERT:
                case UPDATE_AFTER:
                    indexer.add(createIndexRequest(element));
                    break;
                // case UPDATE_BEFORE:
                case DELETE:
                    this.processDelete(element, indexer);
                    break;
                default:
                    throw new TableException("Unsupported message kind: " + element.getRowKind());
            }
        }

        private void processDelete(RowData row, RequestIndexer indexer) {
            Map<String, Object> esVals = getESVals(row);
            Object pkVal = esVals.get(this.pkName);
            if (pkVal == null) {
                throw new IllegalStateException("indexer:" + this.targetIndexName
                        + " relevant pk:" + this.pkName
                        + " can not be null, esVals:"
                        + esVals.entrySet().stream().map((entry) -> entry.getKey() + ":" + entry.getValue()).collect(Collectors.joining(",")));
            }
            //    DeleteRequest deleteRequest = this.requestFactory.createDeleteRequest(this.indexGenerator.generate(row), this.docType, key);
            indexer.add(new DeleteRequest[]{Requests.deleteRequest(this.targetIndexName).id(String.valueOf(pkVal))});
        }
    }


    @TISExtension
    public static class DefaultSinkFunctionDescriptor extends BaseSinkFunctionDescriptor {
        @Override
        public String getDisplayName() {
            return DISPLAY_NAME_FLINK_CDC_SINK;
        }

        @Override
        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {

            ElasticSearchSinkFactory sinkFactory = postFormVals.newInstance();

            return super.verify(msgHandler, context, postFormVals);
        }

        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            return super.validateAll(msgHandler, context, postFormVals);
        }

        @Override
        public PluginVender getVender() {
            return PluginVender.TIS;
        }

        @Override
        protected IEndTypeGetter.EndType getTargetType() {
            return IEndTypeGetter.EndType.ElasticSearch;
        }
    }
}
