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

package com.qlangtech.tis.plugin.datax.hdfs;

import com.qlangtech.tis.fs.IPath;
import com.qlangtech.tis.fs.IPathInfo;
import com.qlangtech.tis.fs.ITISFileSystem;
import com.qlangtech.tis.plugin.tdfs.ITDFSSession;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-08-06 22:26
 **/
public class HdfsTDFSSession implements ITDFSSession {
    private final HdfsTDFDLinker dfsLinker;

    HdfsTDFSSession(HdfsTDFDLinker dfsLinker) {
        this.dfsLinker = Objects.requireNonNull(dfsLinker, "dfsLinker can not be null");
    }

    private ITISFileSystem getFs() {
        return this.dfsLinker.getFs().getFileSystem();
    }

    @Override
    public boolean isDirExist(String directoryPath) {
        return true;
    }

    @Override
    public void mkDirRecursive(String directoryPath) {

    }

    @Override
    public Set<String> getAllFilesInDir(String path, String fileName) {
        ITISFileSystem fs = getFs();
        List<IPathInfo> matchChild = fs.listChildren(fs.getPath(path), (p) -> StringUtils.startsWith(p.getName(), fileName));
        return matchChild.stream().map((p) -> p.getName()).collect(Collectors.toSet());
    }

    @Override
    public void deleteFiles(Set<String> filesToDelete) {
        ITISFileSystem fs = getFs();
        try {
            for (String path : filesToDelete) {
                fs.delete(fs.getPath(path));
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public HashSet<String> getAllFiles(List<String> srcPaths, int parentLevel, int maxTraversalLevel) {
        HashSet<String> paths = new HashSet<>();
        for (String path : srcPaths) {
            paths.addAll(getListFiles(path, parentLevel, maxTraversalLevel));
        }
        return paths;
    }

    @Override
    public HashSet<String> getListFiles(String directoryPath, int parentLevel, int maxTraversalLevel) {
        ITISFileSystem fs = getFs();
        HashSet<String> sourceFiles = new HashSet<>();
        this.getListFiles(sourceFiles, fs, fs.getPath(directoryPath), parentLevel, maxTraversalLevel);
        return sourceFiles;
    }

    private void getListFiles(HashSet<String> sourceFiles, ITISFileSystem fs, IPath directoryPath, int parentLevel, int maxTraversalLevel) {
        if (parentLevel < maxTraversalLevel) {
            List<IPathInfo> children = fs.listChildren(directoryPath);
            for (IPathInfo path : children) {
                if (path.isDir()) {
                    getListFiles(sourceFiles, fs, path.getPath(), parentLevel + 1, maxTraversalLevel);
                } else {
                    sourceFiles.add(String.valueOf(path.getPath()));
                }
            }
        }
    }

    @Override
    public OutputStream getOutputStream(String filePath, boolean append) {
        ITISFileSystem fs = getFs();
        try {
            return fs.create(fs.getPath(filePath), !append);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public InputStream getInputStream(String filePath) {
        ITISFileSystem fs = getFs();
        return fs.open(fs.getPath(filePath));
    }

    @Override
    public void close() throws Exception {
        this.dfsLinker.getFs().getFileSystem().close();
    }
}
