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
//import org.apache.flink.test.util.AbstractTestBase;

import com.alibaba.datax.plugin.writer.hudi.HudiWriter;
import com.qlangtech.plugins.incr.flink.junit.TISApplySkipFlinkClassloaderFactoryCreation;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.async.message.client.consumer.IMQListener;
import com.qlangtech.tis.async.message.client.consumer.impl.MQListenerFactory;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.hdfs.test.HdfsFileSystemFactoryTestUtils;
import com.qlangtech.tis.manage.common.CenterResource;
import com.qlangtech.tis.manage.common.Config;
import com.qlangtech.tis.plugin.KeyedPluginStore;
import com.qlangtech.tis.plugin.incr.IncrStreamFactory;
import com.qlangtech.tis.plugin.incr.TISSinkFactory;
import com.qlangtech.tis.plugins.incr.flink.connector.hudi.HudiSinkFactory;
import com.qlangtech.tis.realtime.BasicFlinkSourceHandle;
import com.qlangtech.tis.realtime.transfer.DTO;
import com.qlangtech.tis.test.TISEasyMock;
import com.qlangtech.tis.util.HeteroEnum;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.test.util.AbstractTestBase;
import org.easymock.EasyMock;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-03-30 15:19
 **/
public class TestTISFlinkCDCStart extends AbstractTestBase implements TISEasyMock {
    TargetResName dataxName = new TargetResName("hudi");

    @ClassRule(order = 100)
    public static TestRule name = new TISApplySkipFlinkClassloaderFactoryCreation();

    @BeforeClass
    public static void beforeClass() {
        CenterResource.setNotFetchFromCenterRepository();
    }

    @Test
    public void testProcessFlinkSourceHandle() throws Throwable {

        String table1 = "totalpayinfo";
        String shortName = TISSinkFactory.KEY_PLUGIN_TPI_CHILD_PATH + dataxName.getName();
        File pluginDir = new File(Config.getPluginLibDir(TISSinkFactory.KEY_PLUGIN_TPI_CHILD_PATH + dataxName.getName()), "../..");
        pluginDir = pluginDir.toPath().normalize().toFile();

        TIS.get().pluginManager.dynamicLoad(shortName, pluginDir, true, null);
        // IDataxProcessor processor = this.mock("dataXprocess", IDataxProcessor.class);

        BasicFlinkSourceHandle hudiHandle
                = TISFlinkCDCStart.createFlinkSourceHandle(dataxName.getName());

        IncrStreamFactory streamFactory = mock("streamFactory", IncrStreamFactory.class);

        EasyMock.expect(streamFactory.createStreamExecutionEnvironment())
                .andReturn(StreamExecutionEnvironment.getExecutionEnvironment());


        Map<IDataxProcessor.TableAlias, SinkFunction<DTO>> sinkFuncts = Collections.singletonMap(new IDataxProcessor.TableAlias(table1), null);
        HudiSinkFactory sinkFactory = new HudiSinkFactory() {
            @Override
            public Map<IDataxProcessor.TableAlias, SinkFunction<DTO>> createSinkFunction(IDataxProcessor dataxProcessor) {
                //  return super.createSinkFunction(dataxProcessor);
                return sinkFuncts;
            }
        };
        sinkFactory.dumpTimeStamp = String.valueOf(HudiWriter.timestamp);
        sinkFactory.currentLimit = 200;
        sinkFactory.setKey(new KeyedPluginStore.Key(null, HdfsFileSystemFactoryTestUtils.testDataXName.getName(), null));


        hudiHandle.setStreamFactory(streamFactory);
        hudiHandle.setSinkFuncFactory(sinkFactory);

        //  AsyncMsg<List<ReaderSource>> asyncMsg = this.mock("asyncMsg", AsyncMsg.class);
        //EasyMock.expect(asyncMsg.getFocusTabs()).andReturn(Collections.singleton(table1)).anyTimes();
        //EasyMock.expect(asyncMsg.getSource()).andReturn(Collections.emptyList());

        this.replay();

        Thread.sleep(5000);

        //  hudiHandle.consume(dataxName, asyncMsg, processor);
        MQListenerFactory listenerFactory = createMQListenerFactory(hudiHandle);
        IMQListener listener = listenerFactory.create();
        // TargetResName dataxName, IDataxReader rdbmsReader, List<ISelectedTab> tabs, IDataxProcessor dataXProcessor

        DataxProcessor processor = DataxProcessor.load(null, dataxName.getName());
        IDataxReader reader = processor.getReader(null);

        listener.start(dataxName, reader, reader.getSelectedTabs(), processor);

        this.verifyAll();
    }

    private MQListenerFactory createMQListenerFactory(BasicFlinkSourceHandle hudiHandle) {


        List<MQListenerFactory> plugins = HeteroEnum.MQ.getPlugins(IPluginContext.namedContext(dataxName.getName()), null);
        for (MQListenerFactory p : plugins) {
            p.setConsumerHandle(hudiHandle);
            return p;
        }

        throw new RuntimeException();


//        Descriptor mySQLDesc = TIS.get().getDescriptor("FlinkCDCMySQLSourceFactory");
//        Objects.requireNonNull(mySQLDesc, "mySQLDesc can not be null");
//        Descriptor.FormData formData = new Descriptor.FormData();
//        formData.addProp("startupOptions", "latest");
//
//
//        Descriptor.ParseDescribable<Describable> parseDescribable
//                = mySQLDesc.newInstance(dataxName.getName(), formData);
//
//        MQListenerFactory listenerFactory = parseDescribable.getInstance();
//        listenerFactory.setConsumerHandle(hudiHandle);
//        return listenerFactory;
    }
}
