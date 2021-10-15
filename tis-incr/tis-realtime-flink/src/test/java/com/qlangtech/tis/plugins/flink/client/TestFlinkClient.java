/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 *   This program is free software: you can use, redistribute, and/or modify
 *   it under the terms of the GNU Affero General Public License, version 3
 *   or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 *  This program is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *   FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.tis.plugins.flink.client;

import com.google.common.collect.Lists;
import junit.framework.TestCase;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.configuration.RestOptions;
import org.easymock.EasyMock;

import java.io.File;
import java.net.URL;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-10-11 09:23
 **/
public class TestFlinkClient extends TestCase {

    public void testSubmitJar() throws Exception {
        Configuration configuration = new Configuration();
        int port = 32710;
        configuration.setString(JobManagerOptions.ADDRESS, "192.168.28.201");
        configuration.setInteger(JobManagerOptions.PORT, port);
        configuration.setInteger(RestOptions.PORT, port);
        RestClusterClient restClient = new RestClusterClient<>(configuration, "my-first-flink-cluster");
//        restClient.setPrintStatusDuringExecution(true);
//        restClient.setDetached(true);

        FlinkClient flinkClient = new FlinkClient();
        JarLoader jarLoader = EasyMock.createMock("jarLoader", JarLoader.class);

//        File streamJar = new File("/tmp/TopSpeedWindowing.jar");
        File streamJar = new File("/Users/mozhenghua/j2ee_solution/project/plugins/tis-incr/tis-flink-cdc-plugin/target/tis-flink-cdc-plugin.jar");

        assertTrue("streamJar must be exist", streamJar.exists());
        EasyMock.expect(jarLoader.downLoad(EasyMock.anyString(), EasyMock.eq(true))).andReturn(streamJar);
        flinkClient.setJarLoader(jarLoader);

        JarSubmitFlinkRequest request = new JarSubmitFlinkRequest();
        request.setCache(true);
        request.setDependency(streamJar.getAbsolutePath());
        request.setParallelism(1);
        request.setEntryClass("com.qlangtech.plugins.incr.flink.cdc.test.TISFlinkCDCMysqlSourceFunction");
        List<URL> classPaths = Lists.newArrayList();
        classPaths.add((new File("/Users/mozhenghua/j2ee_solution/project/plugins/tis-incr/tis-elasticsearch7-sink-plugin/target/tis-elasticsearch7-sink-plugin.jar")).toURL());
        classPaths.add((new File("/Users/mozhenghua/j2ee_solution/project/plugins/tis-incr/tis-realtime-flink/target/tis-realtime-flink.jar")).toURL());
        request.setUserClassPaths(classPaths);

        EasyMock.replay(jarLoader);
        AtomicBoolean launchResult = new AtomicBoolean();
        flinkClient.submitJar(restClient, request, (r) -> {
            launchResult.set(r.isSuccess());
        });

        assertTrue("launchResult must success", launchResult.get());

        EasyMock.verify(jarLoader);
    }
}
