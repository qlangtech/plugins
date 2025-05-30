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

package com.qlangtech.plugins.incr.flink.launch;

import com.qlangtech.plugins.incr.flink.TISApplySkipFlinkClassloaderFactoryCreation;
import com.qlangtech.plugins.incr.flink.launch.clustertype.Standalone;
import com.qlangtech.tis.config.k8s.ReplicasSpec;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.DataXName;
import com.qlangtech.tis.manage.common.CenterResource;
import com.qlangtech.tis.plugin.incr.TISSinkFactory;
import com.qlangtech.tis.util.PluginMeta;
import com.qlangtech.tis.util.RobustReflectionConverter;
import com.qlangtech.tis.util.RobustReflectionConverter2;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TestRule;

import java.util.Set;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-03-28 18:27
 **/
public class TestFlinkTaskNodeController {

    @BeforeClass
    public static void beforeClass() {
        CenterResource.setNotFetchFromCenterRepository();
    }

    @Test
    public void testGetterIncrSinkFactory() {

        DataXName dataX = DataXName.createDataXPipeline("mysql_mysql");
        TISSinkFactory incrSinkFactory = TISSinkFactory.getIncrSinKFactory(dataX);

        Assert.assertNotNull(incrSinkFactory);
        RobustReflectionConverter2.PluginMetas pluginMetas = RobustReflectionConverter2.usedPluginInfo.get();
        Assert.assertTrue(pluginMetas.getMetas().size() > 0);
    }

    @ClassRule(order = 100)
    public static TestRule name = new TISApplySkipFlinkClassloaderFactoryCreation();
    /**
     * 测试重新启动
     *
     * @throws Exception
     */
    @Test
    public void testRelaunch() throws Exception {
        TISFlinkCDCStreamFactory streamFactory = new TISFlinkCDCStreamFactory();
        streamFactory.parallelism = 1;
        Standalone standaloneType = new Standalone();
        standaloneType.flinkCluster = "flink_201";
        streamFactory.cluster = standaloneType;
        String appName = "mysql";
        streamFactory.relaunch(new TargetResName(appName), "file:/opt/data/savepoint/savepoint_20240910121820391/savepoint-e6ccfc-39b00aff0272");
    }

    @Test
    public void testDeploy() throws Exception {

        TISFlinkCDCStreamFactory streamFactory = new TISFlinkCDCStreamFactory();
        // streamFactory.flinkCluster = "my-first-flink-cluster";
        streamFactory.parallelism = 1;
        // FlinkTaskNodeController taskNodeController = new FlinkTaskNodeController(streamFactory);

        TargetResName collection = new TargetResName("hudi");
        ReplicasSpec replicasSpec = new ReplicasSpec();
        long timestamp = 20220325135114l;
        streamFactory.deploy(collection, replicasSpec, timestamp);


//        Map<String, Object> accumulators = taskNodeController.getAccumulators();
//
//        for (Map.Entry<String, Object> entry : accumulators.entrySet()) {
//            System.out.println(entry.getKey());
//        }
    }

}
