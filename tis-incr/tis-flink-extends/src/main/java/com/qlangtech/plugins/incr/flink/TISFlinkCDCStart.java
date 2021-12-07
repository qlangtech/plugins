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

package com.qlangtech.plugins.incr.flink;

import com.qlangtech.tis.async.message.client.consumer.IMQListener;
import com.qlangtech.tis.async.message.client.consumer.impl.MQListenerFactory;
import com.qlangtech.tis.config.k8s.ReplicasSpec;
import com.qlangtech.tis.coredefine.module.action.IRCController;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.incr.IncrStreamFactory;
import com.qlangtech.tis.plugin.incr.TISSinkFactory;
import com.qlangtech.tis.realtime.BasicFlinkSourceHandle;
import com.qlangtech.tis.sql.parser.stream.generate.MergeData;
import com.qlangtech.tis.util.HeteroEnum;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-10-12 10:26
 **/
public class TISFlinkCDCStart {
    // static final String dataxName = "mysql_elastic";
    public static final String TIS_APP_NAME = "tis_app_name";
    private static final Logger logger = LoggerFactory.getLogger(TISFlinkCDCStart.class);

    public static String convertCfgPropertyKey(String key, boolean serialize) {
        return serialize ?
                StringUtils.replace(key, ".", "_")
                : StringUtils.replace(key, "_", ".");
    }

    public static void main(String[] args) throws Exception {

        if (args.length != 1) {
            throw new IllegalArgumentException("args length must be 1,now is:" + args.length);
        }
        String dataxName = args[0];
        //-classpath /Users/mozhenghua/j2ee_solution/project/plugins/tis-incr/tis-flink-dependency/target/tis-flink-dependency/WEB-INF/lib/*:/Users/mozhenghua/j2ee_solution/project/plugins/tis-incr/tis-flink-cdc-plugin/target/tis-flink-cdc-plugin/WEB-INF/lib/*:/Users/mozhenghua/j2ee_solution/project/plugins/tis-incr/tis-elasticsearch7-sink-plugin/target/tis-elasticsearch7-sink-plugin/WEB-INF/lib/*:/Users/mozhenghua/j2ee_solution/project/plugins/tis-incr/tis-realtime-flink/target/tis-realtime-flink/WEB-INF/lib/*:/Users/mozhenghua/j2ee_solution/project/plugins/tis-incr/tis-realtime-flink-launch/target/tis-realtime-flink-launch.jar:/Users/mozhenghua/j2ee_solution/project/plugins/tis-incr/tis-realtime-flink-launch/target/dependency/*:/Users/mozhenghua/j2ee_solution/project/plugins/tis-datax/tis-datax-elasticsearch-plugin/target/tis-datax-elasticsearch-plugin/WEB-INF/lib/*:
        // CenterResource.setNotFetchFromCenterRepository();
        //Thread.currentThread().setContextClassLoader(TIS.get().pluginManager.uberClassLoader);

        IPluginContext pluginContext = IPluginContext.namedContext(dataxName);


        List<IncrStreamFactory> streamFactories = HeteroEnum.INCR_STREAM_CONFIG.getPlugins(pluginContext, null);
        IRCController incrController = null;
        for (IncrStreamFactory factory : streamFactories) {
            incrController = factory.getIncrSync();
        }
        Objects.requireNonNull(incrController, "stream app:" + dataxName + " incrController can not not be null");
        BasicFlinkSourceHandle tableStreamHandle = createFlinkSourceHandle(dataxName);
        deploy(new TargetResName(dataxName), tableStreamHandle, null, -1);
    }

    private static BasicFlinkSourceHandle createFlinkSourceHandle(String dataxName) {
        String streamSourceHandlerClass
                = "com.qlangtech.tis.realtime.transfer." + dataxName + "." + MergeData.getJavaName(dataxName) + "SourceHandle";
        try {
            ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
            Class<?> aClass = Class.forName(streamSourceHandlerClass, true, classLoader);
            return (BasicFlinkSourceHandle) aClass.newInstance();
        } catch (Exception e) {
            throw new RuntimeException(streamSourceHandlerClass, e);
        }
    }


    private static void deploy(TargetResName dataxName, BasicFlinkSourceHandle tableStreamHandle, ReplicasSpec incrSpec, long timestamp) throws Exception {
        // FlinkUserCodeClassLoaders
        // BasicFlinkSourceHandle tisFlinkSourceHandle = new TISFlinkSourceHandle();
        if (tableStreamHandle == null) {
            throw new IllegalStateException("tableStreamHandle has not been instantiated");
        }
        // ElasticSearchSinkFactory esSinkFactory = new ElasticSearchSinkFactory();

        IPluginContext pluginContext = IPluginContext.namedContext(dataxName.getName());
        List<TISSinkFactory> sinkFactories = TISSinkFactory.sinkFactory.getPlugins(pluginContext, null);
        TISSinkFactory sinkFactory = null;
        logger.info("sinkFactories size:" + sinkFactories.size());
        for (TISSinkFactory factory : sinkFactories) {
            sinkFactory = factory;
            break;
        }
        Objects.requireNonNull(sinkFactory, "sinkFactories.size():" + sinkFactories.size());

        tableStreamHandle.setSinkFuncFactory(sinkFactory);

        List<MQListenerFactory> mqFactories = HeteroEnum.MQ.getPlugins(pluginContext, null);
        MQListenerFactory mqFactory = null;
        for (MQListenerFactory factory : mqFactories) {
            factory.setConsumerHandle(tableStreamHandle);
            mqFactory = factory;
        }
        Objects.requireNonNull(mqFactory, "mqFactory can not be null, mqFactories size:" + mqFactories.size());

        IMQListener mq = mqFactory.create();

        IDataxProcessor dataXProcess = DataxProcessor.load(null, dataxName.getName());

        DataxReader reader = (DataxReader) dataXProcess.getReader(null);
        if (reader == null) {
            throw new IllegalStateException("dataXReader is illegal");
        }
        //  DBConfigGetter rdbmsReader = (DBConfigGetter) reader;

        List<ISelectedTab> tabs = reader.getSelectedTabs();
        mq.start(dataxName, reader, tabs, dataXProcess);
    }
}
