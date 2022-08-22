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


import com.qlangtech.org.apache.http.HttpHost;
import com.qlangtech.tis.compiler.incr.ICompileAndPackage;
import com.qlangtech.tis.compiler.streamcode.CompileAndPackage;
import com.qlangtech.tis.config.aliyun.IHttpToken;
import com.qlangtech.tis.datax.IDataXPluginMeta;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.datax.impl.ESTableAlias;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.DataXElasticsearchWriter;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.realtime.BasicTISSinkFactory;
import com.qlangtech.tis.realtime.TabSinkFunc;
import com.qlangtech.tis.realtime.transfer.DTO;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.annotation.Public;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.connectors.elasticsearch.ActionRequestFailureHandler;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkFunction;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.ElasticsearchSink;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.client.Requests;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.*;
import java.util.stream.Collectors;


/**
 * https://ci.apache.org/projects/flink/flink-docs-master/zh/docs/connectors/datastream/elasticsearch/
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-09-28 19:45
 **/
@Public
public class ElasticSearchSinkFactory extends BasicTISSinkFactory<DTO> {
    public static final String DISPLAY_NAME_FLINK_CDC_SINK = "Flink-ElasticSearch-Sink";
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchSinkFactory.class);
    private static final int DEFAULT_PARALLELISM = 1;// parallelism
    // bulk.flush.max.actions
    @FormField(ordinal = 0, type = FormFieldType.INT_NUMBER, validate = Validator.integer)
    public Integer bulkFlushMaxActions;

    @FormField(ordinal = 1, type = FormFieldType.INT_NUMBER, validate = Validator.integer)
    public Integer bulkFlushMaxSizeMb;

    @FormField(ordinal = 2, type = FormFieldType.INT_NUMBER, validate = Validator.integer)
    public Integer bulkFlushIntervalMs;


    @Override
    public Map<IDataxProcessor.TableAlias, TabSinkFunc<DTO>> createSinkFunction(IDataxProcessor dataxProcessor) {

        DataXElasticsearchWriter dataXWriter = (DataXElasticsearchWriter) dataxProcessor.getWriter(null);
        Objects.requireNonNull(dataXWriter, "dataXWriter can not be null");
        IHttpToken token = dataXWriter.getToken();

        ESTableAlias esSchema = null;
        for (Map.Entry<String, IDataxProcessor.TableAlias> e : dataxProcessor.getTabAlias().entrySet()) {
            IDataxProcessor.TableAlias value = e.getValue();
            if (!(value instanceof ESTableAlias)) {
                throw new IllegalStateException("value must be type of 'ESTableAlias',but now is :" + value.getClass());
            }
            esSchema = (ESTableAlias) value;
            break;
        }
        Objects.requireNonNull(esSchema, "esSchema can not be null");
        List<ISelectedTab.ColMeta> cols = esSchema.getSourceCols();
        if (CollectionUtils.isEmpty(cols)) {
            throw new IllegalStateException("cols can not be null");
        }
        Optional<ISelectedTab.ColMeta> firstPK = cols.stream().filter((c) -> c.isPk()).findFirst();
        if (!firstPK.isPresent()) {
            throw new IllegalStateException("has not set PK col");
        }

        /********************************************************
         * 初始化索引Schema
         *******************************************************/
        dataXWriter.initialIndex(esSchema);
//        JSONArray schemaCols = esSchema.getSchemaCols();
//        ESClient esClient = new ESClient();
//        esClient.createClient(token.getEndpoint(),
//                token.getAccessKeyId(),
//                token.getAccessKeySecret(),
//                false,
//                300000,
//                false,
//                false);
//        try {
//            esClient.createIndex(dataXWriter.getIndexName()
//                    , dataXWriter.type
//                    , esClient.genMappings(schemaCols, dataXWriter.type, (columnList) -> {
//                    }), dataXWriter.settings, false);
//        } catch (Exception e) {
//            throw new RuntimeException(e);
//        } finally {
//            try {
//                esClient.closeJestClient();
//            } catch (Throwable e) {
//
//            }
//        }
        //if (!) {
        // throw new IllegalStateException("create index or mapping failed indexName:" + dataXWriter.getIndexName());
        //}


//        Map<String, String> config = new HashMap<>();
//        config.put("cluster.name", "my-cluster-name");
//// This instructs the sink to emit after every element, otherwise they would be buffered
//        config.put("bulk.flush.max.actions", "1");

        List<HttpHost> transportAddresses = new ArrayList<>();
        transportAddresses.add(HttpHost.create(token.getEndpoint()));


        ElasticsearchSink.Builder<DTO> sinkBuilder
                = new ElasticsearchSink.Builder<>(transportAddresses
                , new DefaultElasticsearchSinkFunction(
                cols.stream().map((c) -> c.getName()).collect(Collectors.toSet())
                , firstPK.get().getName()
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

//        new RestClientBuilder.HttpClientConfigCallback() {
//            @Override
//            public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpClientBuilder) {
//                return httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
//            }
//        }

        sinkBuilder.setFailureHandler(new DefaultActionRequestFailureHandler());
        if (StringUtils.isNotEmpty(token.getAccessKeyId())
                || StringUtils.isNotEmpty(token.getAccessKeySecret())) {
            // 如果用户设置了accessKey 或者accessSecret
            sinkBuilder.setRestClientFactory(new TISElasticRestClientFactory(token.getAccessKeyId(), token.getAccessKeySecret()));
//            sinkBuilder.setRestClientFactory(new RestClientFactory() {
//                @Override
//                public void configureRestClientBuilder(RestClientBuilder restClientBuilder) {
//                    final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
//                    credentialsProvider.setCredentials(AuthScope.ANY,
//                            new UsernamePasswordCredentials(token.getAccessKeyId(), token.getAccessKeySecret()));
//                    restClientBuilder.setHttpClientConfigCallback(new RestClientBuilder.HttpClientConfigCallback() {
//                        @Override
//                        public HttpAsyncClientBuilder customizeHttpClient(HttpAsyncClientBuilder httpAsyncClientBuilder) {
//                            return httpAsyncClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
//                        }
//                    });
//                }
//            });
        }


        IDataxProcessor.TableAlias tableMapper = new IDataxProcessor.TableAlias();
        tableMapper.setTo(dataXWriter.getIndexName());
        IDataxReader reader = dataxProcessor.getReader(null);
        for (ISelectedTab selectedTab : reader.getSelectedTabs()) {
            tableMapper.setFrom(selectedTab.getName());
        }
        return Collections.singletonMap(tableMapper, new DTOSinkFunc(tableMapper, sinkBuilder.build(), true, DEFAULT_PARALLELISM));
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
        return new CompileAndPackage();
    }

    private static class DefaultElasticsearchSinkFunction implements ElasticsearchSinkFunction<DTO>, Serializable {
        private final Set<String> cols;
        private final String pkName;
        private final String targetIndexName;

        public DefaultElasticsearchSinkFunction(Set<String> cols, String pkName, String targetIndexName) {
            this.cols = cols;
            this.pkName = pkName;
            this.targetIndexName = targetIndexName;
            if (StringUtils.isEmpty(targetIndexName)) {
                throw new IllegalArgumentException("param targetIndexName can not be null");
            }
        }

        private IndexRequest createIndexRequest(DTO element) {
            Map<String, Object> after = element.getAfter();
            Map<String, Object> json = new HashMap<>();
            Object val = null;
            for (String col : cols) {
                val = after.get(col);
                if (val == null) {
                    continue;
                }
                json.put(col, val);
            }

            Object pkVal = after.get(pkName);

            IndexRequest request = Requests.indexRequest()
                    .index(this.targetIndexName)
                    //.type("my-type")
                    .source(json);
            if (pkVal != null) {
                request.id(String.valueOf(pkVal));
            }
            return request;
        }

        @Override
        public void process(DTO element, RuntimeContext ctx, RequestIndexer indexer) {
            indexer.add(createIndexRequest(element));
        }
    }


    @TISExtension
    public static class DefaultSinkFunctionDescriptor extends BaseSinkFunctionDescriptor {
        @Override
        public String getDisplayName() {
            return DISPLAY_NAME_FLINK_CDC_SINK;
        }

        @Override
        protected IDataXPluginMeta.EndType getTargetType() {
            return IDataXPluginMeta.EndType.ElasticSearch;
        }
    }
}
