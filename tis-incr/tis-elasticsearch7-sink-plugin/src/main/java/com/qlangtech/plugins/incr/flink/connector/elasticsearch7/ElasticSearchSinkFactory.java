/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.plugins.incr.flink.connector.elasticsearch7;


import com.alibaba.datax.plugin.writer.elasticsearchwriter.ESClient;
import com.alibaba.fastjson.JSONArray;
import com.qlangtech.org.apache.http.HttpHost;
import com.qlangtech.tis.config.aliyun.IAliyunToken;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.ISelectedTab;
import com.qlangtech.tis.datax.impl.ESTableAlias;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.DataXElasticsearchWriter;
import com.qlangtech.tis.plugin.incr.TISSinkFactory;
import com.qlangtech.tis.realtime.transfer.DTO;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
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

//import org.apache.flink.types.Row;
//import org.apache.http.HttpHost;

//import com.qlangtech.tis.realtime.transfer.DTO;


/**
 * https://ci.apache.org/projects/flink/flink-docs-master/zh/docs/connectors/datastream/elasticsearch/
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-09-28 19:45
 **/
public class ElasticSearchSinkFactory extends TISSinkFactory {
    public static final String DISPLAY_NAME_FLINK_CDC_SINK = "Flink-CDC-Sink";
    private static final Logger logger = LoggerFactory.getLogger(ElasticSearchSinkFactory.class);
    // bulk.flush.max.actions
    @FormField(ordinal = 0, type = FormFieldType.INT_NUMBER, validate = Validator.integer)
    public Integer bulkFlushMaxActions = 1;

    @FormField(ordinal = 1, type = FormFieldType.INT_NUMBER, validate = Validator.integer)
    public Integer bulkFlushMaxSizeMb;


    @Override
    public SinkFunction<DTO> createSinkFunction(IDataxProcessor dataxProcessor) {

        DataXElasticsearchWriter dataXWriter = (DataXElasticsearchWriter) dataxProcessor.getWriter(null);
        Objects.requireNonNull(dataXWriter, "dataXWriter can not be null");
        IAliyunToken token = dataXWriter.getToken();

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
        JSONArray schemaCols = esSchema.getSchemaCols();
        ESClient esClient = new ESClient();
        esClient.createClient(token.getEndpoint(),
                token.getAccessKeyId(),
                token.getAccessKeySecret(),
                false,
                300000,
                false,
                false);
        try {
            esClient.createIndex(dataXWriter.getIndexName()
                    , dataXWriter.type
                    , esClient.genMappings(schemaCols, dataXWriter.type, (columnList) -> {
                    }), dataXWriter.settings, false);
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                esClient.closeJestClient();
            } catch (Throwable e) {

            }
        }
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

        sinkBuilder.setFailureHandler(new DefaultActionRequestFailureHandler());

        return sinkBuilder.build();
    }

    private static class DefaultActionRequestFailureHandler implements ActionRequestFailureHandler, Serializable {
        @Override
        public void onFailure(ActionRequest actionRequest, Throwable throwable, int restStatusCode, RequestIndexer requestIndexer) throws Throwable {
            //throwable.printStackTrace();
            logger.error(throwable.getMessage(), throwable);
        }
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

            //Set<String> fieldNames = element.getFieldNames(false);
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
    }
}
