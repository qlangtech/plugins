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

package com.qlangtech.plugins.incr.flink;

import com.google.common.collect.Lists;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.async.message.client.consumer.AsyncMsg;
import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.async.message.client.consumer.IMQListener;
import com.qlangtech.tis.async.message.client.consumer.impl.MQListenerFactory;
import com.qlangtech.tis.config.k8s.ReplicasSpec;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.DataXName;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.extension.ExtensionList;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.incr.IncrStreamFactory;
import com.qlangtech.tis.plugin.incr.TISSinkFactory;
import com.qlangtech.tis.realtime.BasicFlinkSourceHandle;
import com.qlangtech.tis.realtime.ReaderSource;
import com.qlangtech.tis.util.HeteroEnum;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-10-12 10:26
 **/
public class TISFlinkCDCStart {

    private static final Logger logger = LoggerFactory.getLogger(TISFlinkCDCStart.class);

    public static void main(String[] args) throws Exception {

        if (args.length != 1) {
            throw new IllegalArgumentException("args length must be 1,now is:" + args.length);
        }
        String dataxName = args[0];

        IncrStreamFactory incrStreamFactory = HeteroEnum.getIncrStreamFactory(dataxName);
        BasicFlinkSourceHandle tableStreamHandle = createFlinkSourceHandle(dataxName);
        tableStreamHandle.setStreamFactory(incrStreamFactory);
        deploy(new TargetResName(dataxName), tableStreamHandle, null, -1);
    }

    public static BasicFlinkSourceHandle createFlinkSourceHandle(String dataxName) {
        TargetResName name = new TargetResName(dataxName);
        final String streamSourceHandlerClass = name.getStreamSourceHandlerClass();
        ExtensionList<BasicFlinkSourceHandle> flinkSourceHandles = TIS.get().getExtensionList(BasicFlinkSourceHandle.class);
        flinkSourceHandles.removeExtensions();
        logger.info("start to load extendsion of " + BasicFlinkSourceHandle.class.getSimpleName());
        List<String> candidatePluginClasses = Lists.newArrayList();
        Optional<BasicFlinkSourceHandle> handle = flinkSourceHandles.stream().filter((p) -> {
            candidatePluginClasses.add(p.getClass().getName());
            return streamSourceHandlerClass.equals(p.getClass().getName());
        }).findFirst();
        if (!handle.isPresent()) {
            throw new IllegalStateException("dataxName:" + dataxName
                    + " relevant " + BasicFlinkSourceHandle.class.getSimpleName() + " is not present in:"
                    + candidatePluginClasses.stream().collect(Collectors.joining(",")));
        }

        return handle.get();
    }


    private static void deploy(TargetResName dataxName
            , BasicFlinkSourceHandle tableStreamHandle
            , ReplicasSpec incrSpec, long timestamp) throws Exception {

        if (tableStreamHandle == null) {
            throw new IllegalStateException("tableStreamHandle has not been instantiated");
        }

        IDataxProcessor dataXProcess = DataxProcessor.load(null, dataxName.getName());
        DataxReader reader = (DataxReader) dataXProcess.getReader(null);
        DataXName dataX = DataXName.createDataXPipeline(dataxName.getName());
        TISSinkFactory incrSinKFactory = TISSinkFactory.getIncrSinKFactory(dataX);
        boolean flinkCDCPipelineEnable = incrSinKFactory.flinkCDCPipelineEnable();
        tableStreamHandle.setSinkFuncFactory(incrSinKFactory);
        tableStreamHandle.setSourceStreamTableMeta(reader);

        MQListenerFactory mqFactory = HeteroEnum.getIncrSourceListenerFactory(dataX);
        tableStreamHandle.setSourceFlinkColCreator(mqFactory.createFlinkColCreator(reader));
        // mqFactory.setConsumerHandle(tableStreamHandle);


        IMQListener mq = mqFactory.create();

        if (reader == null) {
            throw new IllegalStateException("dataXReader is illegal");
        }
        List<ISelectedTab> tabs = reader.getSelectedTabs();
        AsyncMsg<List<ReaderSource>> sourceReaders = mq.start(flinkCDCPipelineEnable, dataxName, reader, tabs, dataXProcess);

        tableStreamHandle.consume(flinkCDCPipelineEnable, dataxName, sourceReaders, dataXProcess);
    }
}
