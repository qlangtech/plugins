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
import com.aliyun.oss.model.GenericResult;
import com.aliyun.oss.model.ListObjectsRequest;
import com.aliyun.oss.model.OSSObject;
import com.aliyun.oss.model.OSSObjectSummary;
import com.aliyun.oss.model.ObjectListing;
import com.aliyun.oss.model.PutObjectRequest;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.qlangtech.tis.fs.IPath;
import com.qlangtech.tis.plugin.tdfs.ITDFSSession;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-08-04 13:43
 **/
public class OSSSession implements ITDFSSession {
    private static final Logger logger = LoggerFactory.getLogger(OSSSession.class);
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
    public String getRootPath() {
        return this.ossLinker.getRootPath();
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
    public HashSet<Res> getAllFiles(List<String> srcPaths, int parentLevel, int maxTraversalLevel) {
        HashSet<Res> allFiles = Sets.newHashSet();
        for (String path : srcPaths) {
            getListFiles(allFiles, path, path, Collections.emptyList(), parentLevel, maxTraversalLevel);
        }
        return allFiles;
    }

    @Override
    public HashSet<Res> getListFiles(String directoryPath, int parentLevel, int maxTraversalLevel) {
        HashSet<Res> allFiles = Sets.newHashSet();
        getListFiles(allFiles, directoryPath, directoryPath, Collections.emptyList(), parentLevel, maxTraversalLevel);
        return allFiles;
    }

    private void getListFiles(HashSet<Res> listFiles, final String rootDir, String directoryPath, List<String> relevantPath, final int parentLevel, final int maxTraversalLevel) {
        if (StringUtils.startsWith(directoryPath, File.separator)) {
            throw new IllegalArgumentException("path:" + directoryPath + " can not start with '" + File.separator + "'");
        }
        if (parentLevel >= maxTraversalLevel) {
            //超出最大递归层数
            String message = String.format("获取path：[%s] 下文件列表时超出最大层数,请确认路径[%s]下不存在软连接文件", directoryPath, directoryPath);
            throw new IllegalStateException(message);
        } else {

            ListObjectsRequest listReq = new ListObjectsRequest();
            listReq.setDelimiter("/");
            listReq.setBucketName(ossLinker.bucket);
            // IPath.pathConcat() 处理很重要，因为OSS取子文件一定要以 '/'结尾 ，才能取到子文件列表
            if (StringUtils.isNotEmpty(directoryPath)) {
                listReq.setPrefix(IPath.pathConcat(directoryPath, StringUtils.EMPTY));
            } else {
                listReq.setPrefix(StringUtils.EMPTY);
            }


            ObjectListing children = this.getOSS().listObjects(listReq);
            // 文件列表
            String fileKey;
            for (OSSObjectSummary obj : children.getObjectSummaries()) {
                fileKey = obj.getKey();
                if (StringUtils.endsWith(fileKey, File.separator)) {
                    continue;
                }
                listFiles.add(new Res(fileKey, Res.buildRelevantPath(relevantPath, StringUtils.substringAfter(fileKey, directoryPath))));
            }
            if (parentLevel + 1 >= maxTraversalLevel) {
                return;
            }

            // 文件夹列表
            for (String commonPrefix : children.getCommonPrefixes()) {
                getListFiles(listFiles, rootDir, commonPrefix, Lists.newArrayList(StringUtils.split(StringUtils.removeStart(commonPrefix, rootDir), File.separatorChar)), parentLevel + 1, maxTraversalLevel);
            }

            // getListFiles(listFiles, null, parentLevel + 1, maxTraversalLevel);
        }
    }

    @Override
    public OutputStream getOutputStream(String filePath, boolean append) {
        PipedOutputStream output = new PipedOutputStream();

        CountDownLatch countDown = new CountDownLatch(1);

        final CountDownLatch over = new CountDownLatch(1);

        ossPutExecutor.execute(() -> {
            try {
                OSS oss = this.getOSS();

                try (PipedInputStream input = new PipedInputStream(output)) {
                    countDown.countDown();

                    GenericResult result = null;
                    if (append) {
                        throw new UnsupportedOperationException("not support with append mode");
                        // result = oss.appendObject(new AppendObjectRequest(ossLinker.bucket, filePath, input));
                    } else {
                        result = oss.putObject(new PutObjectRequest(ossLinker.bucket, filePath, input));
                    }

                }
                over.countDown();
            } catch (Exception e) {
                logger.error("wirte path faild,path:" + filePath, e);
                Thread.currentThread().interrupt();
            }
        });// execute over
        try {
            countDown.await(20, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        return new FilterOutputStream(output) {
            @Override
            public void close() throws IOException {
                super.close();
                try {
                    over.await(2, TimeUnit.HOURS);
                } catch (InterruptedException e) {
                    throw new IOException(e);
                }
            }
        };


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
