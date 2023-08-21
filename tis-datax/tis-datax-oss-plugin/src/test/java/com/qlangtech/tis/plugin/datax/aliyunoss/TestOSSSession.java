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

package com.qlangtech.tis.plugin.datax.aliyunoss;

import com.google.common.collect.Lists;
import com.qlangtech.tis.AliyunOSSCfg;
import com.qlangtech.tis.OssConfig;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.plugin.AliyunEndpoint;
import com.qlangtech.tis.plugin.aliyun.AccessKey;
import com.qlangtech.tis.plugin.tdfs.ITDFSSession;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.List;


/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-08-17 10:33
 **/
public class TestOSSSession {

    private static OSSSession createOSSSession() {
        OssConfig ossCfg = AliyunOSSCfg.getInstance().getOss("hdfs-oss");
        Assert.assertNotNull("ossCfg can not be null", ossCfg);
        AliyunOSSTDFSLinker dfsLinker = new AliyunOSSTDFSLinker() {
            @Override
            protected AliyunEndpoint getOSSConfig() {
                AliyunEndpoint endpoint = new AliyunEndpoint();
                endpoint.endpoint = ossCfg.getEndpoint();
                endpoint.authToken = new AccessKey(ossCfg.getAccessId(), ossCfg.getAccessSecret());

                return endpoint;
            }
        };
        dfsLinker.bucket = ossCfg.getBucket();

        return new OSSSession(dfsLinker);
    }

    @Test
    public void testGetOutputStream() throws Exception {


        try (OSSSession session = createOSSSession()) {
            final String testPath = "user/admin/test";
            final String exampleRes = "oss-datax-reader-assert.json";
            try (OutputStream output = session.getOutputStream(testPath, false)) {
                com.qlangtech.tis.extension.impl.IOUtils.loadResourceFromClasspath(
                        TestOSSSession.class, exampleRes, true, (input) -> {
                            IOUtils.copy(input, output);
                            return null;
                        });

            }

            try (InputStream reader = session.getInputStream(testPath)) {
                Assert.assertEquals(
                        com.qlangtech.tis.extension.impl.IOUtils.loadResourceFromClasspath(TestOSSSession.class, exampleRes)
                        , IOUtils.toString(reader, TisUTF8.get()));
            }
        }


    }

    @Test
    public void testListAllPaths() throws Exception {
        try (OSSSession session = createOSSSession()) {
            List<String> srcPaths = Lists.newArrayList("user/admin/instancedetail");
            HashSet<ITDFSSession.Res> res = session.getAllFiles(srcPaths, 0, 1);
            Assert.assertTrue(res.size() > 0);

            String path = "user/admin";
            srcPaths = Lists.newArrayList(path);
            res = session.getAllFiles(srcPaths, 0, 10);
            for (ITDFSSession.Res r : res) {
                Assert.assertFalse("relevantPath:" + r.relevantPath + " shall not start with:" + path, StringUtils.startsWith(r.relevantPath, path));
            }
            Assert.assertTrue(res.size() > 0);

            final String errorPath = "/user/admin";
            try {
                session.getAllFiles(Lists.newArrayList(errorPath), 0, 10);
                Assert.fail("shall throw a excpetion than can not start with slash");
            } catch (Exception e) {
                Assert.assertEquals("path:" + errorPath + " can not start with '" + File.separator + "'", e.getMessage());
            }
        }
    }
}
