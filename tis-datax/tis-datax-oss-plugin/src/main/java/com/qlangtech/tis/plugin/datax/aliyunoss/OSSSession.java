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

import com.aliyun.oss.OSS;
import com.aliyun.oss.model.*;
import com.google.common.collect.Sets;
import com.qlangtech.tis.plugin.tdfs.ITDFSSession;
import org.apache.commons.collections.CollectionUtils;

import java.io.*;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-08-04 13:43
 **/
public class OSSSession implements ITDFSSession {
    private final AliyunOSSTDFSLinker ossLinker;
    private OSS oss;
    private static final ExecutorService ossPutExecutor = Executors.newCachedThreadPool();

    public OSSSession(AliyunOSSTDFSLinker ossLinker) {
        this.ossLinker = ossLinker;
    }

    private OSS getOSS() {
        if (oss == null) {
            oss = ossLinker.createOSSClient();
        }
        return oss;
    }

    @Override
    public boolean isDirExist(String dir) {
        return true;
    }

    @Override
    public void mkDirRecursive(String directoryPath) {

    }

    @Override
    public Set<String> getAllFilesInDir(String path, String prefixFileName) {
        ObjectListing objectList = this.getOSS().listObjects(ossLinker.bucket, path + prefixFileName);
        List<OSSObjectSummary> objectSummaries = objectList.getObjectSummaries();
        Set<String> result = Sets.newHashSet();
        for (OSSObjectSummary obj : objectSummaries) {
            result.add(obj.getKey());
        }
        return result;
    }

    @Override
    public void deleteFiles(Set<String> filesToDelete) {
        if (CollectionUtils.isEmpty(filesToDelete)) {
            throw new IllegalArgumentException("param filesToDelete can not be empty");
        }
        for (String df : filesToDelete) {
            this.getOSS().deleteObject(ossLinker.bucket, df);
        }
    }

    @Override
    public HashSet<String> getAllFiles(List<String> srcPaths, int parentLevel, int maxTraversalLevel) {
        HashSet<String> allFiles = Sets.newHashSet();
        for (String path : srcPaths) {
            getListFiles(allFiles, path, parentLevel, maxTraversalLevel);
        }
        return allFiles;
    }

    @Override
    public HashSet<String> getListFiles(String directoryPath, int parentLevel, int maxTraversalLevel) {
        HashSet<String> allFiles = Sets.newHashSet();
        getListFiles(allFiles, directoryPath, parentLevel, maxTraversalLevel);
        return allFiles;
    }

    private void getListFiles(HashSet<String> listFiles, String directoryPath, final int parentLevel, final int maxTraversalLevel) {
        if (parentLevel >= maxTraversalLevel) {
            //超出最大递归层数
            String message = String.format("获取path：[%s] 下文件列表时超出最大层数,请确认路径[%s]下不存在软连接文件", directoryPath, directoryPath);
            throw new IllegalStateException(message);
        } else {

            ListObjectsRequest listReq = new ListObjectsRequest();
            listReq.setDelimiter("/");
            listReq.setBucketName(ossLinker.bucket);
            listReq.setPrefix(directoryPath);

            ObjectListing children = this.getOSS().listObjects(listReq);
            // 文件列表
            for (OSSObjectSummary obj : children.getObjectSummaries()) {
                listFiles.add(obj.getKey());
            }
            if (parentLevel + 1 >= maxTraversalLevel) {
                return;
            }
            // 文件夹列表
            for (String commonPrefix : children.getCommonPrefixes()) {
                getListFiles(listFiles, directoryPath + commonPrefix, parentLevel + 1, maxTraversalLevel);
            }

            // getListFiles(listFiles, null, parentLevel + 1, maxTraversalLevel);
        }
    }

    @Override
    public OutputStream getOutputStream(String filePath, boolean append) {
        PipedOutputStream output = new PipedOutputStream();
        ossPutExecutor.execute(() -> {
            OSS oss = this.getOSS();

            try (PipedInputStream input = new PipedInputStream(output)) {
                if (append) {
                    oss.appendObject(new AppendObjectRequest(ossLinker.bucket, filePath, input));
                } else {
                    oss.putObject(new PutObjectRequest(ossLinker.bucket, filePath, input));
                }

            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        return output;


    }

    @Override
    public InputStream getInputStream(String filePath) {
        OSSObject ossObject = this.getOSS().getObject(ossLinker.bucket, filePath);
        return ossObject.getObjectContent();
    }

    @Override
    public void close() throws Exception {
        getOSS().shutdown();
    }
}
