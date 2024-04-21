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

import com.alibaba.datax.plugin.writer.elasticsearchwriter.ESColumn;
import com.alibaba.datax.plugin.writer.elasticsearchwriter.ESFieldType;
import com.google.common.collect.Maps;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.plugins.incr.flink.junit.TISApplySkipFlinkClassloaderFactoryCreation;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.datax.TableAlias;
import com.qlangtech.tis.datax.TableAliasMapper;
import com.qlangtech.tis.datax.impl.ESTableAlias;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.plugin.aliyun.NoneToken;
import com.qlangtech.tis.plugin.datax.DataXElasticsearchWriter;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.datax.elastic.ElasticEndpoint;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.JDBCTypes;
import com.qlangtech.tis.plugin.incr.TISSinkFactory;
import com.qlangtech.tis.plugins.incr.flink.cdc.DTO2RowDataMapper;
import com.qlangtech.tis.realtime.TabSinkFunc;
import com.qlangtech.tis.realtime.dto.DTOStream;
import com.qlangtech.tis.realtime.transfer.DTO;
import com.qlangtech.tis.test.TISEasyMock;
import org.apache.commons.compress.utils.Lists;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.RowData;
import org.apache.flink.test.util.AbstractTestBase;
import org.easymock.EasyMock;
import org.elasticsearch.client.Client;
import org.junit.Assert;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-09-29 16:55
 **/
public class TestElasticSearchSinkFactory<C extends AutoCloseable>
        extends AbstractTestBase implements TISEasyMock {

    @ClassRule(order = 100)
    public static TestRule name = new TISApplySkipFlinkClassloaderFactoryCreation();

    public void testLoadDescriptorLoad() {
        List<Descriptor<TISSinkFactory>> descriptors = TISSinkFactory.sinkFactory.descriptors();
        Assert.assertEquals(1, descriptors.size());
        Assert.assertEquals(ElasticSearchSinkFactory.DISPLAY_NAME_FLINK_CDC_SINK, descriptors.get(0).getDisplayName());
    }

    /**
     * 参考：ElasticsearchSinkTestBase
     *
     * @throws Exception
     */
    @Test
    public void testCreateSinkFunction() throws Exception {

        String tableName = "totalpayinfo";
        String colEntityId = "entity_id";
        String colNum = "num";
        String colId = "id";
        String colCreateTime = "create_time";

        IDataxProcessor dataxProcessor = mock("dataxProcessor", IDataxProcessor.class);

        IDataxReader dataxReader = mock("dataxReader", IDataxReader.class);
        List<ISelectedTab> selectedTabs = Lists.newArrayList();
        ISelectedTab totalpayinfo = mock(tableName, ISelectedTab.class);
        EasyMock.expect(totalpayinfo.getName()).andReturn(tableName);
        List<CMeta> cols = Lists.newArrayList();
        CMeta cm = new CMeta();
        cm.setName(colEntityId);
        cm.setType(DataType.createVarChar(6));
        cols.add(cm);

        cm = new CMeta();
        cm.setName(colNum);
        cm.setType(DataType.getType(JDBCTypes.INTEGER));
        cols.add(cm);

        cm = new CMeta();
        cm.setName(colId);
        cm.setType(DataType.createVarChar(32));
        cm.setPk(true);
        cols.add(cm);

        cm = new CMeta();
        cm.setName(colCreateTime);
        cm.setType(DataType.getType(JDBCTypes.BIGINT));
        cols.add(cm);

        EasyMock.expect(totalpayinfo.getCols()).andReturn(cols).anyTimes();
        selectedTabs.add(totalpayinfo);
        EasyMock.expect(dataxReader.getSelectedTabs()).andReturn(selectedTabs);

        EasyMock.expect(dataxProcessor.getReader(null)).andReturn(dataxReader);


        DataXElasticsearchWriter dataXWriter = mock("dataXWriter", DataXElasticsearchWriter.class);
        EasyMock.expect(dataXWriter.getIndexName()).andReturn(tableName);

        ElasticEndpoint endpoint = new ElasticEndpoint();
        endpoint.endpoint = "http://192.168.28.201:9200";
        endpoint.authToken = new NoneToken();

        EasyMock.expect(dataXWriter.getToken()).andReturn(endpoint);

        //  ESTableAlias esTableAlias = new ESTableAlias();

        ESTableAlias esTableAlias = new ESTableAlias() {
            @Override
            public List<CMeta> getSourceCols() {
                return cols;
            }
        };

        List<ESColumn> esCols = cols.stream().map((c) -> {
            ESColumn esCol = new ESColumn();
            esCol.setName(c.getName());
            esCol.setPk(c.isPk());
            esCol.setType(String.valueOf(ESFieldType.KEYWORD));

            return esCol;
        }).collect(Collectors.toList());

        EasyMock.expect(dataXWriter.initialIndex(esTableAlias)).andReturn(esCols);


        EasyMock.expect(dataxProcessor.getWriter(null)).andReturn(dataXWriter);


        Map<String, TableAlias> aliasMap = new HashMap<>();

        aliasMap.put(tableName, esTableAlias);
        EasyMock.expect(dataxProcessor.getTabAlias(null)).andReturn(new TableAliasMapper(aliasMap));

        this.replay();

        ElasticSearchSinkFactory esSinkFactory = new ElasticSearchSinkFactory();
        Map<TableAlias, TabSinkFunc<RowData>>
                sinkFuncs = esSinkFactory.createSinkFunction(dataxProcessor);
        Assert.assertTrue("sinkFuncs must > 0", sinkFuncs.size() > 0);

        // StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // env.setParallelism(1);

        DTO d = new DTO();
        d.setTableName(tableName);
        d.setEventType(DTO.EventType.ADD);
        Map<String, Object> after = Maps.newHashMap();
        after.put(colEntityId, "334556");
        after.put(colNum, "5");
        after.put(colId, "123dsf124325253dsf123");
        after.put(colCreateTime, "20211113115959");
        d.setAfter(after);
        Assert.assertEquals(1, sinkFuncs.size());

        for (Map.Entry<TableAlias, TabSinkFunc<RowData>> entry : sinkFuncs.entrySet()) {
            // env.fromElements(new DTO[]{d}).addSink(entry.getValue()).name("clickhouse");
            runElasticSearchSinkTest(
                    "elasticsearch-sink-test-json-index", entry.getValue());
            break;
        }


        // env.execute("testJob");

        Thread.sleep(5000);

        this.verifyAll();
        // Client client = getClient();
    }


    // It's not good that we're using a Client here instead of a Rest Client but we need this
    // for compatibility with ES 5.3.x. As soon as we drop that we can use RestClient here.
//    protected abstract Client getClient();
//
//    protected abstract String getClusterName();

    private void runElasticSearchSinkTest(
            String index,
            TabSinkFunc<RowData> sinkFunc)
            throws Exception {
        Objects.requireNonNull(sinkFunc, "sinkFunc can not be null");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<DTO> source =
                env.addSource(new TestDataSourceFunction());

        DTOStream<RowData> sourceStream = DTOStream.createRowData(source.map(new DTO2RowDataMapper(sinkFunc.getSourceColsMeta())));


        sinkFunc.add2Sink(sourceStream);
        //  source.addSink(sinkFunc.);

        env.execute("Elasticsearch Sink Test");

        // verify the results
        Client client = null;// getClient();

        verifyProducedSinkData(client, index);

      //  client.close();
    }

    /**
     * A {@link SourceFunction} that generates the elements (id, "message #" + id) with id being 0 -
     * 20.
     */
    public static class TestDataSourceFunction implements SourceFunction<DTO> {
        private static final long serialVersionUID = 1L;
        private static final int NUM_ELEMENTS = 20;
        private volatile boolean running = true;

        @Override
        public void run(SourceFunction.SourceContext<DTO> ctx) throws Exception {

//             `base_id` int(11) NOT NULL,
//             `start_time` datetime DEFAULT NULL,
//              `update_date` date DEFAULT NULL,
//             `update_time` timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP,
//             `price` decimal(5,2) DEFAULT NULL,
//            `json_content` json DEFAULT NULL,
//             `col_blob` blob,
//              `col_text` text,
            DTO dto = null;
            Map<String, Object> after = null;
            SimpleDateFormat datetimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
            SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd");
            for (int i = 1; i <= NUM_ELEMENTS && running; i++) {
                dto = new DTO();
                dto.setEventType(DTO.EventType.ADD);
                dto.setTableName("base");
                after = Maps.newHashMap();
                after.put("base_id", String.valueOf(i));
                after.put("start_time", datetimeFormat.format(new Date()));
                after.put("update_date", dateFormat.format(new Date()));
                after.put("price", "12.99");
                after.put("json_content", "{\"name\":\"baisui" + i + "\"}");
                dto.setAfter(after);
                ctx.collect(dto);
            }
        }

        @Override
        public void cancel() {
            running = false;
        }
    }


    public static void verifyProducedSinkData(Client client, String index) {
    }

}
