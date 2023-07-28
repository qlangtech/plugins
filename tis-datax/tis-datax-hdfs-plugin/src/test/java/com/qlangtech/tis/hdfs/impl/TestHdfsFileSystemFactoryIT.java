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

import com.qlangtech.tis.config.authtoken.UserTokenUtils;
import com.qlangtech.tis.fs.IPathInfo;
import com.qlangtech.tis.fs.ITISFileSystem;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * @author: baisui 百岁
 * @create: 2020-11-24 17:50
 **/
public class TestHdfsFileSystemFactoryIT {
    @Test
    public void testPluginGet() {

        HdfsFileSystemFactory fsFactory = new HdfsFileSystemFactory();
        fsFactory.rootDir = "/user/admin";
        fsFactory.userToken = UserTokenUtils.createKerberosToken("tis_kerberos");
        fsFactory.hdfsSiteContent //
                = "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n" +
                "\n" +
                "<configuration>\n" +
                "  <property>\n" +
                "    <name>fs.defaultFS</name>\n" +
                "    <value>hdfs://192.168.28.200</value>\n" +
                "  </property>\n" +
                "</configuration>";
        fsFactory.getFileSystem();
        ITISFileSystem hdfs = fsFactory.getFileSystem();
        Assert.assertNotNull(hdfs);
        List<IPathInfo> childpath = hdfs.listChildren(hdfs.getPath("/"));
        childpath.forEach((path) -> {
            System.out.println(path.getName());
        });
        hdfs.close();

//        List<Descriptor<FileSystemFactory>> descList
//                = TIS.get().getDescriptorList(FileSystemFactory.class);
//        assertNotNull(descList);
//        assertEquals(2, descList.size());
//        Set<String> displayNames = Sets.newHashSet("AliyunOSSFileSystemFactory", "HDFS");
//        for (Descriptor<FileSystemFactory> desc : descList) {
//            System.out.println(desc.getDisplayName());
//        }
    }


}
