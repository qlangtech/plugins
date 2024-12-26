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

package com.qlangtech.tis.plugin.datax.tdfs.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.qlangtech.tis.fs.IPath;
import com.qlangtech.tis.fs.IPathInfo;
import com.qlangtech.tis.fs.ITISFileSystem;
import com.qlangtech.tis.plugin.tdfs.ITDFSSession;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOCase;
import org.apache.commons.io.filefilter.FileFilterUtils;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-12-25 16:02
 **/
public class LocalFileDFSSession implements ITDFSSession {

    private final LocalFileDFSLinker dfsLinker;

    public LocalFileDFSSession(LocalFileDFSLinker dfsLinker) {
        this.dfsLinker = Objects.requireNonNull(dfsLinker, "dfsLinker");
    }

    @Override
    public String getRootPath() {
        return dfsLinker.getRootPath();
    }

    @Override
    public boolean isDirExist(String directoryPath) {
        File dirPath = new File(directoryPath);
        return dirPath.exists() && dirPath.isDirectory();
    }

    @Override
    public void mkDirRecursive(String directoryPath) {
        try {
            FileUtils.forceMkdir(new File(directoryPath));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Set<String> getAllFilesInDir(String path, String fileName) {
        Collection<File> childFiles
                = FileUtils.listFiles(new File(path)
                , FileFilterUtils.prefixFileFilter(fileName, IOCase.INSENSITIVE)
                , FileFilterUtils.falseFileFilter());
        return childFiles.stream().map((file) -> file.getName()).collect(Collectors.toSet());
    }

    @Override
    public void deleteFiles(Set<String> filesToDelete) {
        filesToDelete.forEach((file) -> {
            FileUtils.deleteQuietly(new File(file));
        });
    }

    @Override
    public HashSet<Res> getAllFiles(List<String> srcPaths, int parentLevel, int maxTraversalLevel) {
        HashSet<Res> res = new HashSet<>();
        for (String path : srcPaths) {
            getListFiles(res, new File(path), Lists.newArrayList(), parentLevel, maxTraversalLevel);
        }
        return res;
    }

    @Override
    public HashSet<Res> getListFiles(String directoryPath, int parentLevel, int maxTraversalLevel) {
        File findPath = new File(directoryPath);
        HashSet<Res> res = Sets.newHashSet();
        getListFiles(res, findPath, Lists.newArrayList(), parentLevel, maxTraversalLevel);
        return res;
    }

    private void getListFiles(HashSet<Res> sourceFiles
            , File directoryPath, List<String> relevantPaths, int parentLevel, int maxTraversalLevel) {
        if (parentLevel < maxTraversalLevel) {
            File[] children = directoryPath.listFiles();
            for (File path : children) {
                if (path.isDirectory()) {
                    getListFiles(sourceFiles, path
                            , Res.appendElement(relevantPaths, path.getName())
                            , parentLevel + 1, maxTraversalLevel);
                } else {
                    sourceFiles.add(new Res(String.valueOf(path.getPath()), Res.buildRelevantPath(relevantPaths, path.getName())));
                }
            }
        }
    }

    @Override
    public OutputStream getOutputStream(String filePath, boolean append) {
        try {
            return FileUtils.openOutputStream(new File(filePath), append);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public InputStream getInputStream(String filePath) {
        try {
            return FileUtils.openInputStream(new File(filePath));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() throws Exception {

    }
}
