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

package com.qlangtech.plugins.incr.flink.cdc;

import com.google.common.collect.Sets;
import com.qlangtech.plugins.incr.flink.cdc.SourceChannel.HostDBs;
import com.qlangtech.plugins.incr.flink.cdc.SourceChannel.ReaderSourceCreator;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.PostedDSProp;
import com.qlangtech.tis.realtime.ReaderSource;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-09-20 16:20
 **/
public class TestSourceChannel  {

    @Test
    public void testGetSourceFunction() {

        DataSourceFactory dsFactory = TIS.getDataBasePlugin(PostedDSProp.parse("order2"));
        SelectedTab tab = new SelectedTab();
        tab.name = "orderdetail";
        List<ISelectedTab> tabs = Collections.singletonList(tab);
        Set<String> expectTbs = Sets.newHashSet("order2.orderdetail_02", "order2.orderdetail_01", "order1.orderdetail");
        Set<String> actualSplitTabs = Sets.newHashSet();
        ReaderSourceCreator sourceFunctionCreator = new ReaderSourceCreator() {
            @Override
            public List<ReaderSource> create(String dbHost, HostDBs dbs, Set<String> tbs, Properties debeziumProperties) {
                actualSplitTabs.addAll(tbs);
                return Collections.emptyList();
            }
        };

        SourceChannel.getSourceFunction(dsFactory, tabs, sourceFunctionCreator);

        Assert.assertTrue("expect:" + String.join(",", expectTbs) + "\nactual:" + String.join(",", actualSplitTabs)
                , CollectionUtils.isEqualCollection(expectTbs, actualSplitTabs));
    }
}
