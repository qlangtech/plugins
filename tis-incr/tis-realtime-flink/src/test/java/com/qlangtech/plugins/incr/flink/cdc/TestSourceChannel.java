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
import com.qlangtech.plugins.incr.flink.cdc.SourceChannel.ReaderSourceCreator;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.ds.DBConfig.HostDBs;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.PostedDSProp;
import com.qlangtech.tis.plugin.ds.SplitableTableInDB;
import com.qlangtech.tis.plugin.ds.TableInDB;
import com.qlangtech.tis.realtime.ReaderSource;
import org.apache.commons.collections.CollectionUtils;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-09-20 16:20
 **/
public class TestSourceChannel {

    @Test
    public void testGetSourceFunctionXX() {
        Descriptor mysqlV5Desc = TIS.get().getDescriptor("com.qlangtech.tis.plugin.ds.mysql.MySQLV5DataSourceFactory");
        Descriptor.FormData formData = new Descriptor.FormData();

        Descriptor.FormData splitTableData = new Descriptor.FormData();
        splitTableData.addProp("nodeDesc", "rm-2zeraf77tcjw3fyn4.mysal.rds.aliyuncs.com[0008-0010]");
        splitTableData.addProp("tabPattern", "(\\S+)(_\\w+)");
        splitTableData.addProp("testTab", "t_ord_order");
        splitTableData.addProp("prefixWildcardStyle", String.valueOf(false));
        formData.addSubForm("splitTableStrategy"
                , "com.qlangtech.tis.plugin.ds.split.DefaultSplitTableStrategy", splitTableData);
        formData.addProp("port", "3306");
        formData.addProp("name", "test");
        formData.addProp("userName", "tata");
        formData.addProp("password", "123456");
        formData.addProp("dbName", "center_order_yyos_");
        formData.addProp("encode", "utf8");
        formData.addProp("useCompression", String.valueOf(false));
        formData.addSubForm("timeZone", "com.qlangtech.tis.plugin.timezone.DefaultTISTimeZone"
                , new Descriptor.FormData("timeZone", "Asia/Shanghai"));

        DataSourceFactory dsFactory
                = (DataSourceFactory) Objects.requireNonNull(mysqlV5Desc).newInstance(null, formData).getInstance();

        final String logicTabNameOrderDetail = "t_ord_order";
        SelectedTab tab = new SelectedTab();
        tab.name = logicTabNameOrderDetail;
        final String splitTab1 = "orderdetail_02";
        List<ISelectedTab> tabs = Collections.singletonList(tab);

        Set<String> actualSplitTabs = Sets.newHashSet();
        ReaderSourceCreator sourceFunctionCreator = new ReaderSourceCreator() {
            @Override
            public List<ReaderSource> create(String dbHost, HostDBs dbs, Set<String> tbs, Properties debeziumProperties) {
                actualSplitTabs.addAll(tbs);
                return Collections.emptyList();
            }
        };

        List<ReaderSource> sourceFunction = SourceChannel.getSourceFunction(dsFactory, tabs, sourceFunctionCreator);
        System.out.println("sourceFunction size:" + sourceFunction.size());
        System.out.println("actualSplitTabs:" + String.join(",", actualSplitTabs));
    }


    @Test
    public void testGetSourceFunction() {

        DataSourceFactory dsFactory = TIS.getDataBasePlugin(PostedDSProp.parse("order2"));
        final String logicTabNameOrderDetail = "orderdetail";
        SelectedTab tab = new SelectedTab();
        tab.name = logicTabNameOrderDetail;
        final String splitTab1 = "orderdetail_02";
        List<ISelectedTab> tabs = Collections.singletonList(tab);
        Set<String> expectTbs = Sets.newHashSet("order2." + splitTab1, "order2.orderdetail_01", "order1.orderdetail");
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


        TableInDB tableInDB = dsFactory.getTablesInDB();
        Assert.assertNotNull("tableInDB can not be null", tableInDB);
        Assert.assertTrue(tableInDB instanceof SplitableTableInDB);
        SplitableTableInDB splitableTableInDB = (SplitableTableInDB) tableInDB;

        Function<String, String> physicsTabName2LogicNameConvertor = splitableTableInDB.getPhysicsTabName2LogicNameConvertor();
        Assert.assertNotNull(physicsTabName2LogicNameConvertor);

        String logicTabName = physicsTabName2LogicNameConvertor.apply(splitTab1);
        Assert.assertEquals(logicTabNameOrderDetail, logicTabName);
    }
}
