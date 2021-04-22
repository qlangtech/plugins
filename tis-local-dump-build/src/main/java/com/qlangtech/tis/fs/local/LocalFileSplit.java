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

import com.qlangtech.tis.fs.IFileSplit;
import com.qlangtech.tis.fs.IPath;

/**
 * @author: baisui 百岁
 * @create: 2021-03-02 13:25
 **/
public class LocalFileSplit implements IFileSplit {

    private final LocalFilePath local;

    public LocalFileSplit(LocalFilePath local) {
        this.local = local;
    }

    @Override
    public IPath getPath() {
        return local;
    }

    @Override
    public long getStart() {
        return 0;
    }

    @Override
    public long getLength() {
        return local.file.length();
    }
}
