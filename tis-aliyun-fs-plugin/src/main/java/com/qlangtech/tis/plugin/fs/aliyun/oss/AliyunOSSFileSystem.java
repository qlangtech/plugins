/** Copyright 2020 QingLang, Inc.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.qlangtech.tis.plugin.fs.aliyun.oss;

import com.qlangtech.tis.fs.*;
import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

/**
 * @create: 2020-04-12 20:10
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2020/04/13
 */
public class AliyunOSSFileSystem implements ITISFileSystem {

    private final AliyunOSSFileSystemFactory fsFactory;

    public AliyunOSSFileSystem(AliyunOSSFileSystemFactory fsFactory) {
        this.fsFactory = fsFactory;
    }

    @Override
    public String getName() {
        return "aliyun-oss";
    }

    @Override
    public IPath getPath(String path) {
        return null;
    }

    @Override
    public IPath getPath(IPath parent, String name) {
        return null;
    }

    @Override
    public OutputStream getOutputStream(IPath path) {
        return null;
    }

    @Override
    public FSDataInputStream open(IPath path, int bufferSize) {
        return null;
    }

    @Override
    public FSDataInputStream open(IPath path) {
        return null;
    }

    @Override
    public TISFSDataOutputStream create(IPath f, boolean overwrite, int bufferSize) throws IOException {
        return null;
    }

    @Override
    public TISFSDataOutputStream create(IPath f, boolean overwrite) throws IOException {
        return null;
    }

    @Override
    public boolean exists(IPath path) {
        return false;
    }

    @Override
    public boolean mkdirs(IPath f) throws IOException {
        return false;
    }

    @Override
    public void copyToLocalFile(IPath srcPath, File dstPath) {
    }

    @Override
    public void rename(IPath from, IPath to) {
    }

    @Override
    public boolean copyFromLocalFile(File localIncrPath, IPath remoteIncrPath) {
        return false;
    }

    @Override
    public IFileSplitor getSplitor(IPath path) throws Exception {
        return null;
    }

    @Override
    public IContentSummary getContentSummary(IPath path) {
        return null;
    }

    @Override
    public List<IPathInfo> listChildren(IPath path) {
        return null;
    }

    @Override
    public List<IPathInfo> listChildren(IPath path, IPathFilter filter) {
        return null;
    }

    @Override
    public IPathInfo getFileInfo(IPath path) {
        return null;
    }

    @Override
    public boolean delete(IPath f, boolean recursive) throws IOException {
        return false;
    }

    @Override
    public boolean delete(IPath f) throws IOException {
        return false;
    }

    @Override
    public void close() {
    }
}
