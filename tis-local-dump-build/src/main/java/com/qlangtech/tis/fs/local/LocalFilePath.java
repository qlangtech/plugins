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

import com.qlangtech.tis.fs.IPath;

import java.io.File;

/**
 * @author: baisui 百岁
 * @create: 2021-03-02 13:05
 **/
public class LocalFilePath implements IPath {
    final File file;

    public LocalFilePath(File file) {
        this.file = file;
    }

    @Override
    public String getName() {
        return file.getName();
    }

    @Override
    public <T> T unwrap(Class<T> iface) {
        if (iface != File.class) {
            throw new IllegalStateException("iface:" + iface + " must be " + File.class.getName());
        }
        return (T) file;
    }
}
