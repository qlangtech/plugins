///**
// * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
// * <p>
// *   This program is free software: you can use, redistribute, and/or modify
// *   it under the terms of the GNU Affero General Public License, version 3
// *   or later ("AGPL"), as published by the Free Software Foundation.
// * <p>
// *  This program is distributed in the hope that it will be useful, but WITHOUT
// *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
// *   FITNESS FOR A PARTICULAR PURPOSE.
// * <p>
// *  You should have received a copy of the GNU Affero General Public License
// *  along with this program. If not, see <http://www.gnu.org/licenses/>.
// */
//
//package com.qlangtech.plugins.incr.flink;
//
//import com.qlangtech.plugins.incr.flink.launch.TISFlinkCDCStreamFactory;
//import com.qlangtech.tis.async.message.client.consumer.IMQListener;
//import com.qlangtech.tis.async.message.client.consumer.impl.MQListenerFactory;
//import com.qlangtech.tis.config.k8s.ReplicasSpec;
//import com.qlangtech.tis.coredefine.module.action.IRCController;
//import com.qlangtech.tis.coredefine.module.action.RcDeployment;
//import com.qlangtech.tis.coredefine.module.action.TargetResName;
//import com.qlangtech.tis.datax.IDataxProcessor;
//import com.qlangtech.tis.datax.ISelectedTab;
//import com.qlangtech.tis.datax.impl.DataxProcessor;
//import com.qlangtech.tis.datax.impl.DataxReader;
//import com.qlangtech.tis.plugin.ds.DBConfigGetter;
//import com.qlangtech.tis.plugin.incr.TISSinkFactory;
//import com.qlangtech.tis.plugin.incr.WatchPodLog;
//import com.qlangtech.tis.realtime.BasicFlinkSourceHandle;
//import com.qlangtech.tis.realtime.FlinkSourceHandleSetter;
//import com.qlangtech.tis.trigger.jst.ILogListener;
//import com.qlangtech.tis.util.HeteroEnum;
//import com.qlangtech.tis.util.IPluginContext;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.util.List;
//import java.util.Objects;
//
///**
// * @author: 百岁（baisui@qlangtech.com）
// * @create: 2021-10-12 10:00
// **/
//public class FlinkTaskNodeController implements IRCController, FlinkSourceHandleSetter {
//    private static final Logger logger = LoggerFactory.getLogger(FlinkTaskNodeController.class);
//
//    private BasicFlinkSourceHandle tableStreamHandle;
//
//    private MQListenerFactory mqFactory;
//
//    private final TISFlinkCDCStreamFactory factory;
//
//    public FlinkTaskNodeController(TISFlinkCDCStreamFactory factory) {
//        this.factory = factory;
//    }
//
//    @Override
//    public void setTableStreamHandle(BasicFlinkSourceHandle tableStreamHandle) {
//        this.tableStreamHandle = tableStreamHandle;
//    }
//
//    @Override
//    public void deploy(TargetResName dataxName, ReplicasSpec incrSpec, long timestamp) throws Exception {
//        // FlinkUserCodeClassLoaders
//        // BasicFlinkSourceHandle tisFlinkSourceHandle = new TISFlinkSourceHandle();
//        if (tableStreamHandle == null) {
//            throw new IllegalStateException("tableStreamHandle has not been instantiated");
//        }
//        // ElasticSearchSinkFactory esSinkFactory = new ElasticSearchSinkFactory();
//
//        IPluginContext pluginContext = IPluginContext.namedContext(dataxName.getName());
//        List<TISSinkFactory> sinkFactories = TISSinkFactory.sinkFactory.getPlugins(pluginContext, null);
//        logger.info("sinkFactories size:" + sinkFactories.size());
//        if (sinkFactories.size() != 1) {
//            throw new IllegalStateException("sinkFactories.size() must be 1");
//        }
//        // ElasticSearchSinkFactory esSinkFactory = (ElasticSearchSinkFactory) sinkFactories.get(0);
//
//        tableStreamHandle.setSinkFuncFactory(sinkFactories.get(0));
//
//        List<MQListenerFactory> mqFactories = HeteroEnum.MQ.getPlugins(pluginContext, null);
//        MQListenerFactory mqFactory = null;
//        for (MQListenerFactory factory : mqFactories) {
//            factory.setConsumerHandle(tableStreamHandle);
//            mqFactory = factory;
//        }
//        Objects.requireNonNull(mqFactory, "mqFactory can not be null, mqFactories size:" + mqFactories.size());
//
//        IMQListener mq = mqFactory.create();
//
//        IDataxProcessor dataXProcess = DataxProcessor.load(null, dataxName.getName());//  EasyMock.createMock("dataXProcessor", IDataxProcessor.class);
//
//
//        DataxReader reader = (DataxReader) dataXProcess.getReader(null);
//        if (reader == null || !(reader instanceof DBConfigGetter)) {
//            throw new IllegalStateException("dataXReader is illegal");
//        }
//        DBConfigGetter rdbmsReader = (DBConfigGetter) reader;
//
//        List<ISelectedTab> tabs = reader.getSelectedTabs();
//        mq.start(rdbmsReader, tabs, dataXProcess);
//    }
//
//    @Override
//    public void removeInstance(TargetResName collection) throws Exception {
//
//    }
//
//    @Override
//    public void relaunch(TargetResName collection, String... targetPod) {
//
//    }
//
//    @Override
//    public RcDeployment getRCDeployment(TargetResName collection) {
//        // throw new UnsupportedOperationException();
//        return null;
//    }
//
//    @Override
//    public WatchPodLog listPodAndWatchLog(TargetResName collection, String podName, ILogListener listener) {
//        throw new UnsupportedOperationException();
//    }
//
//    private static ISelectedTab.ColMeta addCol(String colName, ISelectedTab.DataXReaderColType type) {
//        return addCol(colName, type, false);
//    }
//
//    private static ISelectedTab.ColMeta addCol(String colName, ISelectedTab.DataXReaderColType type, boolean pk) {
//        ISelectedTab.ColMeta col = new ISelectedTab.ColMeta();
//        col.setName(colName);
//        col.setType(type);
//        col.setPk(pk);
//        return col;
//    }
//}
