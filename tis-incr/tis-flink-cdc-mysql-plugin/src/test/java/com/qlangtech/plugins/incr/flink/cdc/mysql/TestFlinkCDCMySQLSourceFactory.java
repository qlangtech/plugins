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

package com.qlangtech.plugins.incr.flink.cdc.mysql;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.qlangtech.plugins.incr.flink.TISFlinClassLoaderFactory;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.manage.common.CenterResource;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsReader;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.incr.TISSinkFactory;
import com.qlangtech.tis.realtime.BasicFlinkSourceHandle;
import com.qlangtech.tis.realtime.transfer.DTO;
import com.qlangtech.tis.test.TISEasyMock;
import junit.framework.TestCase;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.easymock.EasyMock;

import java.util.*;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-12-18 09:57
 **/
public class TestFlinkCDCMySQLSourceFactory extends TestCase implements TISEasyMock {

    @Override
    public void setUp() throws Exception {
        CenterResource.setNotFetchFromCenterRepository();
        System.setProperty(TISFlinClassLoaderFactory.SKIP_CLASSLOADER_FACTORY_CREATION, "true");
    }

    public void testDate() {

        long date = 1639831884000l;


        System.out.println(MySQLSourceValConvert.TIME_FORMAT.get().format(new Date(date)));

    }

    public void testBinlogConsume() throws Exception {


        FlinkCDCMySQLSourceFactory mysqlCDCFactory = new FlinkCDCMySQLSourceFactory();
        mysqlCDCFactory.startupOptions = "latest";
        final String tabName = "base";
        TargetResName dataxName = new TargetResName("mysql_startrock2");

        BasicFlinkSourceHandle consumerHandle = new TestBasicFlinkSourceHandle(tabName);

        TISSinkFactory sinkFuncFactory = new TISSinkFactory() {
            @Override
            public Map<IDataxProcessor.TableAlias, SinkFunction<DTO>> createSinkFunction(IDataxProcessor dataxProcessor) {
                Map<IDataxProcessor.TableAlias, SinkFunction<DTO>> result = Maps.newHashMap();
                result.put(new IDataxProcessor.TableAlias(tabName), new PrintSinkFunction());
                return result;
            }
        };
        consumerHandle.setSinkFuncFactory(sinkFuncFactory);
        mysqlCDCFactory.setConsumerHandle(consumerHandle);

        FlinkCDCMysqlSourceFunction imqListener = (FlinkCDCMysqlSourceFunction) mysqlCDCFactory.create();


        BasicDataXRdbmsReader dataxReader = (BasicDataXRdbmsReader) DataxReader.load(null, dataxName.getName());
        Objects.requireNonNull(dataxReader);
//        IDataxReader rdbmsReader =

        List<SelectedTab> selectedTabs = dataxReader.getSelectedTabs();
        Optional<SelectedTab> firstSelectedTab = selectedTabs.stream().filter((t) -> tabName.equals(t.name)).findFirst();
        assertTrue("firstSelectedTab must be present", firstSelectedTab.isPresent());

         ISelectedTab tab = firstSelectedTab.get();
//        EasyMock.expect(tab.getName()).andReturn(tabName).anyTimes();
//        List<ISelectedTab.ColMeta> cols = Lists.newArrayList();
//        ISelectedTab.ColMeta col = new ISelectedTab.ColMeta();
//        col.setName("base_id");
//        col.setPk(true);
//        col.setType(DataType.dat);
//        cols.add(col);



        List<ISelectedTab> tabs = Collections.singletonList(tab);
        IDataxProcessor dataXProcessor = mock("dataXProcessor", IDataxProcessor.class);
        replay();
        imqListener.start(dataxName, dataxReader, tabs, dataXProcessor);
        Thread.sleep(10000);
        verifyAll();
    }
}
