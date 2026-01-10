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

package com.qlangtech.tis.plugins.incr.flink.chunjun.source;

import com.dtstack.chunjun.conf.ContentConf;
import com.dtstack.chunjun.conf.JobConf;
import com.dtstack.chunjun.conf.OperatorConf;
import com.dtstack.chunjun.conf.SyncConf;
import com.dtstack.chunjun.connector.jdbc.source.JdbcSourceFactory;
import com.dtstack.chunjun.constants.ConfigConstant;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.qlangtech.plugins.incr.flink.cdc.SourceChannel;
import com.qlangtech.plugins.incr.flink.launch.TISFlinkCDCStreamFactory;
import com.qlangtech.tis.async.message.client.consumer.AsyncMsg;
import com.qlangtech.tis.async.message.client.consumer.IMQListener;
import com.qlangtech.tis.async.message.client.consumer.MQConsumeException;
import com.qlangtech.tis.datax.DataXJobInfo;
import com.qlangtech.tis.datax.DataXJobSubmit;
import com.qlangtech.tis.datax.DataXName;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.datax.IStreamTableMeta;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsReader;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.TableInDB;
import com.qlangtech.tis.plugin.incr.IConsumerRateLimiter;
import com.qlangtech.tis.plugin.incr.IncrStreamFactory;
import com.qlangtech.tis.realtime.ReaderSource;
import com.qlangtech.tis.realtime.dto.DTOStream;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.data.RowData;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-08-10 11:59
 **/
public abstract class ChunjunSourceFunction
        implements IMQListener<List<ReaderSource>> {
    final ChunjunSourceFactory sourceFactory;
    IncrStreamFactory streamFactory = new TISFlinkCDCStreamFactory();

    public ChunjunSourceFunction(ChunjunSourceFactory sourceFactory) {
        this.sourceFactory = sourceFactory;
    }


//    @Override
//    public IConsumerHandle getConsumerHandle() {
//        return this.sourceFactory.getConsumerHandle();
//    }

    private SourceFunction<RowData> createSourceFunction(
            IDataxProcessor.TableMap tableMap, SyncConf conf, BasicDataSourceFactory sourceFactory, BasicDataXRdbmsReader reader) {

        AtomicReference<SourceFunction<RowData>> sourceFunc = new AtomicReference<>();
        IStreamTableMeta streamTableMeta = reader.getStreamTableMeta(tableMap);
        JdbcSourceFactory chunjunSourceFactory = createChunjunSourceFactory(conf, sourceFactory, streamTableMeta, sourceFunc);
        Objects.requireNonNull(chunjunSourceFactory, "chunjunSourceFactory can not be null");
        chunjunSourceFactory.createSource();
        return Objects.requireNonNull(sourceFunc.get(), "SourceFunction<RowData> shall present");
    }

    protected abstract JdbcSourceFactory //
    createChunjunSourceFactory( //
                                SyncConf conf, BasicDataSourceFactory sourceFactory
            , IStreamTableMeta streamTableMeta, AtomicReference<SourceFunction<RowData>> sourceFunc);


    @Override
    public AsyncMsg<List<ReaderSource>> start(IConsumerRateLimiter streamFactory, boolean flinkCDCPipelineEnable, DataXName names, IDataxReader dataSource
            , List<ISelectedTab> tabs, IDataxProcessor dataXProcessor) throws MQConsumeException {
        Objects.requireNonNull(dataXProcessor, "dataXProcessor can not be null");
        BasicDataXRdbmsReader reader = (BasicDataXRdbmsReader) dataSource;
        final BasicDataSourceFactory sourceFactory = (BasicDataSourceFactory) reader.getDataSourceFactory();
        TableInDB tabsInDB = sourceFactory.getTablesInDB();
        List<ReaderSource> sourceFuncs = Lists.newArrayList();

        sourceFactory.getDbConfig().vistDbURL(false, (dbName, dbHost, jdbcUrl) -> {
            DataXJobInfo dataXJobInfo = null;
            Optional<String[]> targetPhysicsNames = null;
            List<String> physicsNames = null;
            for (ISelectedTab tab : tabs) {
                dataXJobInfo = tabsInDB.createDataXJobInfo(DataXJobSubmit.TableDataXEntity.createTableEntity(null, jdbcUrl, tab.getName()), false);
                targetPhysicsNames = dataXJobInfo.getTargetTableNames();
                if (targetPhysicsNames.isPresent()) {
                    physicsNames = Lists.newArrayList(targetPhysicsNames.get());
                } else {
                    physicsNames = Collections.singletonList(tab.getName());
                }

                for (String physicsName : physicsNames) {
                    SyncConf conf = createSyncConf(sourceFactory, jdbcUrl, dbName, (SelectedTab) tab, physicsName);
                    SourceFunction<RowData> sourceFunc = createSourceFunction(dataXProcessor.findTableMap(null, tab.getName()), conf, sourceFactory, reader);
                    sourceFuncs.add(ReaderSource.createRowDataSource(streamFactory, names,
                            dbHost + ":" + sourceFactory.port + "_" + dbName + "." + physicsName, tab, sourceFunc));
                }
            }
        });


        try {
            SourceChannel sourceChannel = new SourceChannel(false, sourceFuncs);
            sourceChannel.setFocusTabs(tabs, dataXProcessor, (tabName) -> DTOStream.createRowData());
            return sourceChannel;
            // return (JobExecutionResult) getConsumerHandle().consume(false, name, sourceChannel, dataXProcessor);
        } catch (Exception e) {
            throw new MQConsumeException(e.getMessage(), e);
        }
    }

    private SyncConf createSyncConf(BasicDataSourceFactory sourceFactory, String jdbcUrl, String dbName, SelectedTab tab, String physicsTabName) {
        SyncConf syncConf = new SyncConf();

        JobConf jobConf = new JobConf();
        ContentConf content = new ContentConf();
        OperatorConf writer = new OperatorConf();
        Map<String, Object> writerParams = Maps.newHashMap();
        writer.setParameter(writerParams);

        OperatorConf reader = new OperatorConf();
        reader.setName("postgresqlreader");
        Map<String, Object> params = Maps.newHashMap();
        params.put("username", sourceFactory.getUserName());
        params.put("password", sourceFactory.getPassword());

        params.put("queryTimeOut", this.sourceFactory.queryTimeOut);
        params.put("fetchSize", this.sourceFactory.fetchSize);

        //tab.getIncrMode().set(params);

        List<Map<String, String>> cols = Lists.newArrayList();
        Map<String, String> col = null;
        // com.dtstack.chunjun.conf.FieldConf.getField(List)

        for (CMeta cm : tab.getCols()) {
            col = Maps.newHashMap();
            col.put("name", cm.getName());
            col.put("type", parseType(cm));
            cols.add(col);
        }

        params.put(ConfigConstant.KEY_COLUMN, cols);
        params.put("fullColumn", tab.getCols().stream().map((c) -> c.getName()).collect(Collectors.toList()));
        Map<String, Object> conn = Maps.newHashMap();
        conn.put("jdbcUrl", Collections.singletonList(jdbcUrl));
        conn.put("table", Lists.newArrayList(physicsTabName));
        //  conn.put("schema", dbName);
        params.put("connection", Lists.newArrayList(conn));

        SelectedTabPropsExtends tabExtend = tab.getIncrSourceProps();
        if (tabExtend == null) {
            throw new IllegalStateException("tab:" + tab.getName() + " relevant tabExtend can not be null");
        }
        // tabExtend.polling.setParams(params);
        tabExtend.setParams(params);

        // polling
//        params.put("polling", true);
//        params.put("pollingInterval", 3000);
//        params.put("increColumn", "id");
//        params.put("startLocation", 0);

        reader.setParameter(params);
        content.setReader(reader);
        content.setWriter(writer);
        jobConf.setContent(Lists.newLinkedList(Collections.singleton(content)));
        syncConf.setJob(jobConf);
        return syncConf;
    }

    protected final String parseType(CMeta cm) {
        return cm.getType().getS();
    }

}
