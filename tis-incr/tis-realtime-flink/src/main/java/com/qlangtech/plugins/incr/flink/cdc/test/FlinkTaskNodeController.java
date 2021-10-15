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

package com.qlangtech.plugins.incr.flink.cdc.test;

import com.qlangtech.tis.async.message.client.consumer.IMQListener;
import com.qlangtech.tis.async.message.client.consumer.impl.MQListenerFactory;
import com.qlangtech.tis.config.k8s.ReplicasSpec;
import com.qlangtech.tis.coredefine.module.action.IRCController;
import com.qlangtech.tis.coredefine.module.action.RcDeployment;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.ISelectedTab;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.plugin.ds.DBConfigGetter;
import com.qlangtech.tis.plugin.incr.TISSinkFactory;
import com.qlangtech.tis.plugin.incr.WatchPodLog;
import com.qlangtech.tis.realtime.BasicFlinkSourceHandle;
import com.qlangtech.tis.realtime.FlinkSourceHandleSetter;
import com.qlangtech.tis.trigger.jst.ILogListener;
import com.qlangtech.tis.util.HeteroEnum;
import com.qlangtech.tis.util.IPluginContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-10-12 10:00
 **/
public class FlinkTaskNodeController implements IRCController, FlinkSourceHandleSetter {
    private static final Logger logger = LoggerFactory.getLogger(FlinkTaskNodeController.class);

    private BasicFlinkSourceHandle tableStreamHandle;

    @Override
    public void setTableStreamHandle(BasicFlinkSourceHandle tableStreamHandle) {
        this.tableStreamHandle = tableStreamHandle;
    }

    @Override
    public void deploy(TargetResName dataxName, ReplicasSpec incrSpec, long timestamp) throws Exception {
        // FlinkUserCodeClassLoaders
        // BasicFlinkSourceHandle tisFlinkSourceHandle = new TISFlinkSourceHandle();
        if (tableStreamHandle == null) {
            throw new IllegalStateException("tableStreamHandle has not been instantiated");
        }
        // ElasticSearchSinkFactory esSinkFactory = new ElasticSearchSinkFactory();

        IPluginContext pluginContext = IPluginContext.namedContext(dataxName.getName());
        List<TISSinkFactory> sinkFactories = TISSinkFactory.sinkFactory.getPlugins(pluginContext, null);
        logger.info("sinkFactories size:" + sinkFactories.size());
        if (sinkFactories.size() != 1) {
            throw new IllegalStateException("sinkFactories.size() must be 1");
        }
        // ElasticSearchSinkFactory esSinkFactory = (ElasticSearchSinkFactory) sinkFactories.get(0);

        tableStreamHandle.setSinkFuncFactory(sinkFactories.get(0));

        List<MQListenerFactory> mqFactories = HeteroEnum.MQ.getPlugins(pluginContext, null);
        logger.info("mqFactories size:" + mqFactories.size());
        if (mqFactories.size() != 1) {
            throw new IllegalStateException("mqFactories.size() must be 1");
        }
        for (MQListenerFactory mqFactory : mqFactories) {
            mqFactory.setConsumerHandle(tableStreamHandle);
        }

        MQListenerFactory mqFactory = mqFactories.get(0);

//        FlinkCDCMySQLSourceFactory factory = new FlinkCDCMySQLSourceFactory() {
//            @Override
//            public IConsumerHandle getConsumerHander() {
//                return tisFlinkSourceHandle;
//            }
//        };
        //factory.startupOptions = "latest";
        // com.qlangtech.plugins.incr.flink.cdc.mysql.FlinkCDCMysqlSourceFunction mq = (com.qlangtech.plugins.incr.flink.cdc.mysql.FlinkCDCMysqlSourceFunction) factory.create();

        IMQListener mq = mqFactory.create();

        IDataxProcessor dataXProcess = DataxProcessor.load(null, dataxName.getName());//  EasyMock.createMock("dataXProcessor", IDataxProcessor.class);

        //  Map<String, IDataxProcessor.TableAlias> tableAlias = Maps.newHashMap();
        // List<ISelectedTab.ColMeta> cols = Lists.newArrayList();
//        ESTableAlias esTab = new ESTableAlias() {
//            @Override
//            public List<ISelectedTab.ColMeta> getSourceCols() {
//                return cols;
//            }
//        };
//        esTab.setFrom(tabWaitinginstanceinfo);
//        esTab.setTo(tabWaitinginstanceinfo);
//
//        cols.add(addCol("waitingorder_id", ISelectedTab.DataXReaderColType.STRING, true));
//        cols.add(addCol("order_id", ISelectedTab.DataXReaderColType.STRING));
//        cols.add(addCol("entity_id", ISelectedTab.DataXReaderColType.STRING));
//        cols.add(addCol("is_valid", ISelectedTab.DataXReaderColType.INT));
//        cols.add(addCol("last_ver", ISelectedTab.DataXReaderColType.INT));

        //esTab.setSourceCols(cols);
        // tableAlias.put(tabWaitinginstanceinfo, esTab);
        //  EasyMock.expect(dataXProcess.getTabAlias()).andReturn(tableAlias);

        //  DataXElasticsearchWriter esWriter = EasyMock.createMock("elasticWriter", DataXElasticsearchWriter.class);
        // EasyMock.expect(dataXProcess.getWriter(null)).andReturn(esWriter);

        // IAliyunToken httpToken = EasyMock.createMock("httpToken", IAliyunToken.class);

        // EasyMock.expect(httpToken.getEndpoint()).andReturn("http://192.168.28.201:9200");
        // EasyMock.expect(esWriter.getToken()).andReturn(httpToken);
        // EasyMock.expect(esWriter.getIndexName()).andReturn(tabWaitinginstanceinfo);

        // mqFactory.setDataXProcessor(dataXProcess);

//        BasicDataSourceFactory dataSource = new BasicDataSourceFactory() {
//            @Override
//            public String buidJdbcUrl(DBConfig db, String ip, String dbName) {
//                return null;
//            }
//        };
//        dataSource.nodeDesc = "192.168.28.200[2],192.168.28.201[2]";
//        dataSource.port = 3306;
//        dataSource.dbName = "order";
//        dataSource.userName = "root";
//        dataSource.password = "123456";
//        dataSource.encode = "utf8";

        DataxReader reader = (DataxReader) dataXProcess.getReader(null);
        if (reader == null || !(reader instanceof DBConfigGetter)) {
            throw new IllegalStateException("dataXReader is illegal");
        }
        DBConfigGetter rdbmsReader = (DBConfigGetter) reader;

        List<ISelectedTab> tabs = reader.getSelectedTabs();
//        List<ISelectedTab> tabs = Lists.newArrayList();
//        TestSelectedTab tab = new TestSelectedTab(tabWaitinginstanceinfo, cols);
//        tabs.add(tab);

        //  EasyMock.replay(dataXProcess, esWriter, httpToken);
        mq.start(rdbmsReader, tabs, dataXProcess);
    }

    @Override
    public void removeInstance(TargetResName collection) throws Exception {

    }

    @Override
    public void relaunch(TargetResName collection, String... targetPod) {

    }

    @Override
    public RcDeployment getRCDeployment(TargetResName collection) {
        throw new UnsupportedOperationException();
    }

    @Override
    public WatchPodLog listPodAndWatchLog(TargetResName collection, String podName, ILogListener listener) {
        throw new UnsupportedOperationException();
    }

    private static ISelectedTab.ColMeta addCol(String colName, ISelectedTab.DataXReaderColType type) {
        return addCol(colName, type, false);
    }

    private static ISelectedTab.ColMeta addCol(String colName, ISelectedTab.DataXReaderColType type, boolean pk) {
        ISelectedTab.ColMeta col = new ISelectedTab.ColMeta();
        col.setName(colName);
        col.setType(type);
        col.setPk(pk);
        return col;
    }
}
