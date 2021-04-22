/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 *   This program is free software: you can use, redistribute, and/or modify
 *   it under the terms of the GNU Affero General Public License, version 3
 *   or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 *  This program is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *   FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.tis.fs.local;

import com.qlangtech.tis.fs.IPath;
import com.qlangtech.tis.fs.IPathInfo;
import org.apache.commons.io.FileUtils;

/**
 * @author: baisui 百岁
 * @create: 2021-03-02 15:15
 **/
public class LocalPathInfo implements IPathInfo {

    private final LocalFilePath path;
    private final long sizeOf;

    public LocalPathInfo(LocalFilePath path) {
        this.path = path;
        this.sizeOf = FileUtils.sizeOf(path.file);
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
        return this.sizeOf;
    }
}
