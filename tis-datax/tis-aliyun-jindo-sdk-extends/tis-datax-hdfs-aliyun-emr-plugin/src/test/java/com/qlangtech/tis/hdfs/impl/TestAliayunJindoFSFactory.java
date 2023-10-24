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

package com.qlangtech.tis.hdfs.impl;

import com.qlangtech.tis.AliyunOSSCfg;
import com.qlangtech.tis.OssConfig;
import com.qlangtech.tis.config.authtoken.impl.OffUserToken;
import com.qlangtech.tis.fs.*;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.plugin.AliyunEndpoint;
import com.qlangtech.tis.plugin.aliyun.AccessKey;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-07-27 14:19
 **/
public class TestAliayunJindoFSFactory {


    @Test
    public void testListChild() throws Exception {

        AliyunOSSCfg ossCfgs = AliyunOSSCfg.getInstance();
        OssConfig ossCfg = ossCfgs.getOss("hdfs-oss");

        AliyunEndpoint aliyunEndpoint = new AliyunEndpoint();
        AccessKey accessKey = new AccessKey();
        accessKey.accessKeySecret = ossCfg.getAccessSecret();
        accessKey.accessKeyId = ossCfg.getAccessId();
        aliyunEndpoint.authToken = accessKey;
        aliyunEndpoint.endpoint = StringUtils.substringAfter(ossCfg.getEndpoint(), "//");

        // final String dftFSAddress = "oss://" + ossCfg.getBucket() + "/";

        AliayunJindoFSFactory fsFactory = new AliayunJindoFSFactory() {
            @Override
            protected AliyunEndpoint getAliyunEndpoint() {
                return aliyunEndpoint;
            }
        };
        fsFactory.bucket = ossCfg.getBucket();
        fsFactory.userToken = new OffUserToken();
        fsFactory.hdfsSiteContent = AliayunJindoFSFactory.nullHdfsSiteContent();
        fsFactory.rootDir = "/test";

        ITISFileSystem fs = fsFactory.getFileSystem();

        IPath path = fs.getPath("/");

        List<IPathInfo> childPath = fs.listChildren(path);
        System.out.println("show childPath");
        for (IPathInfo c : childPath) {
            System.out.println(c.getName());
        }
        IPath newPath = fs.getPath("/new_file");
        try (TISFSDataOutputStream output = fs.create(newPath, true)) {
            IOUtils.write(AliayunJindoFSFactory.nullHdfsSiteContent(), output, TisUTF8.get());
        }

        try (FSDataInputStream reader = fs.open(newPath)) {
            Assert.assertNotNull("reader can not be null", reader);
            Assert.assertEquals(AliayunJindoFSFactory.nullHdfsSiteContent(), IOUtils.toString(reader, TisUTF8.get()));
        }

    }
}
