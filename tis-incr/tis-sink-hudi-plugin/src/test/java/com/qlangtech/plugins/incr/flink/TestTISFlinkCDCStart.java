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

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.async.message.client.consumer.AsyncMsg;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.manage.common.CenterResource;
import com.qlangtech.tis.manage.common.Config;
import com.qlangtech.tis.plugin.incr.IncrStreamFactory;
import com.qlangtech.tis.plugin.incr.TISSinkFactory;
import com.qlangtech.tis.realtime.BasicFlinkSourceHandle;
import com.qlangtech.tis.realtime.ReaderSource;
import com.qlangtech.tis.test.TISEasyMock;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.easymock.EasyMock;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-03-30 15:19
 **/
public class TestTISFlinkCDCStart implements TISEasyMock {

    @BeforeClass
    public static void beforeClass() {
        CenterResource.setNotFetchFromCenterRepository();
    }

    @Test
    public void testProcessFlinkSourceHandle() throws Throwable {
        TargetResName dataxName = new TargetResName("hudi");
        String table1 = "totalpayinfo";
        String shortName = TISSinkFactory.KEY_PLUGIN_TPI_CHILD_PATH + dataxName.getName();
        File pluginDir = new File(Config.getPluginLibDir(TISSinkFactory.KEY_PLUGIN_TPI_CHILD_PATH + dataxName.getName()), "../..");

        TIS.get().pluginManager.dynamicLoad(shortName, pluginDir, true, null);
        IDataxProcessor processor = this.mock("dataXprocess", IDataxProcessor.class);

        BasicFlinkSourceHandle hudiHandle
                = TISFlinkCDCStart.createFlinkSourceHandle(dataxName.getName());

        IncrStreamFactory streamFactory = mock("streamFactory", IncrStreamFactory.class);

        EasyMock.expect(streamFactory.createStreamExecutionEnvironment())
                .andReturn(StreamExecutionEnvironment.getExecutionEnvironment());

        TISSinkFactory sinkFactory = mock("sinkFactory", TISSinkFactory.class);

        //  SinkFunction<DTO>
        Map<IDataxProcessor.TableAlias, Object> sinkFuncts = Collections.singletonMap(new IDataxProcessor.TableAlias(table1), null);

        EasyMock.expect(sinkFactory.createSinkFunction(processor)).andReturn(sinkFuncts);

        hudiHandle.setStreamFactory(streamFactory);
        hudiHandle.setSinkFuncFactory(sinkFactory);

//        TargetResName dataxName, AsyncMsg<List<ReaderSource>> asyncMsg
//                , IDataxProcessor dataXProcessor
        AsyncMsg<List<ReaderSource>> asyncMsg = this.mock("asyncMsg", AsyncMsg.class);
        EasyMock.expect(asyncMsg.getFocusTabs()).andReturn(Collections.singleton(table1)).anyTimes();
        EasyMock.expect(asyncMsg.getSource()).andReturn(Collections.emptyList());


        this.replay();


        Thread.sleep(5000);

        hudiHandle.consume(dataxName, asyncMsg, processor);

        this.verifyAll();
    }
}
