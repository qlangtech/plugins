/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package com.qlangtech.tis.fs.local;

import com.google.common.collect.Lists;
import com.qlangtech.tis.fs.*;
import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Collections;
import java.util.List;

/**
 * 基于本地文件系统的FileSystem实现
 *
 * @author: baisui 百岁
 * @create: 2021-03-02 13:00
 **/
public class LocalFileSystem implements ITISFileSystem {
    private static final String NAME_FS = "localFileSystem";
    private final String rootDir;

    public LocalFileSystem(String rootDir) {
        this.rootDir = rootDir;
    }

    @Override
    public String getRootDir() {
        return this.rootDir;
    }

    @Override
    public String getName() {
        return NAME_FS;
    }

    @Override
    public IPath getPath(String path) {
        return new LocalFilePath(new File(path));
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
        return open(path);
    }

    @Override
    public FSDataInputStream open(IPath path) {
        File local = getUnwrap(path);
        try {
            return new LocalFSDataInputStream(local);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public TISFSDataOutputStream create(IPath f, boolean overwrite, int bufferSize) throws IOException {
        return new LocalDataOutputStream(getUnwrap(f), overwrite);
    }

    @Override
    public TISFSDataOutputStream create(IPath f, boolean overwrite) throws IOException {
        return new LocalDataOutputStream(getUnwrap(f), overwrite);
    }

    @Override
    public boolean exists(IPath path) {
        File local = getUnwrap(path);
        return local.exists();
    }

    private File getUnwrap(IPath path) {
        return path.unwrap(File.class);
    }

    @Override
    public boolean mkdirs(IPath f) throws IOException {
        File local = getUnwrap(f);
        FileUtils.forceMkdir(local);
        return true;
    }

    @Override
    public void copyToLocalFile(IPath srcPath, File dstPath) {
        File local = getUnwrap(srcPath);
        if (!local.exists()) {
            throw new IllegalStateException("local file:" + local.getAbsolutePath() + " is not exist");
        }
        try {
            FileUtils.copyFile(local, dstPath);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void rename(IPath from, IPath to) {

    }

    @Override
    public boolean copyFromLocalFile(File localIncrPath, IPath remoteIncrPath) {
        throw new UnsupportedOperationException();
    }

    @Override
    public IFileSplitor getSplitor(IPath path) throws Exception {
        File local = getUnwrap(path);
        return new IFileSplitor() {
            @Override
            public List<IFileSplit> getSplits(IndexBuildConfig config) throws Exception {
                return Collections.singletonList(new LocalFileSplit((LocalFilePath) path));
            }

            @Override
            public long getTotalSize() {
                return local.length();
            }
        };
    }

    @Override
    public IContentSummary getContentSummary(IPath path) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<IPathInfo> listChildren(IPath path) {
        return listChildren(path, (p) -> true);
    }

    @Override
    public List<IPathInfo> listChildren(IPath path, IPathFilter filter) {
        LocalFilePath lpath = (LocalFilePath) path;

        if (!lpath.file.exists()) {
            throw new IllegalStateException("file:" + lpath.file + " is not exist");
        }
        List<IPathInfo> childInfos = Lists.newArrayList();
        LocalFilePath lfpath = null;
        for (String child : lpath.file.list()) {
            lfpath = new LocalFilePath(new File(lpath.file, child));
            if (!filter.accept(lfpath)) {
                continue;
            }
            childInfos.add(new LocalPathInfo(lfpath));
        }
        return childInfos;
    }

    @Override
    public IPathInfo getFileInfo(IPath path) {
        return new LocalPathInfo((LocalFilePath) path);
    }

    @Override
    public boolean delete(IPath f, boolean recursive) throws IOException {
        return delete(f);
    }

    @Override
    public boolean delete(IPath f) throws IOException {
        File local = getUnwrap(f);
        FileUtils.deleteQuietly(local);
        return true;
    }

    @Override
    public void close() {

    }
}
