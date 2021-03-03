package com.qlangtech.tis.fs.local;

import com.qlangtech.tis.fs.IPath;
import com.qlangtech.tis.fs.IPathInfo;

/**
 * @author: baisui 百岁
 * @create: 2021-03-02 15:15
 **/
public class LocalPathInfo implements IPathInfo {

    private final LocalFilePath path;

    public LocalPathInfo(LocalFilePath path) {
        this.path = path;
    }

    @Override
    public String getName() {
        return path.getName();
    }

    @Override
    public IPath getPath() {
        return this.path;
    }

    @Override
    public boolean isDir() {
        return path.file.isDirectory();
    }

    @Override
    public long getModificationTime() {
        return path.file.lastModified();
    }

    @Override
    public long getLength() {
        return 0;
    }
}
