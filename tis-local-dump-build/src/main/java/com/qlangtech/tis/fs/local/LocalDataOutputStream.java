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

import com.qlangtech.tis.fs.TISFSDataOutputStream;
import org.apache.commons.io.FileUtils;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.IOException;

/**
 * @author: baisui 百岁
 * @create: 2021-03-02 15:38
 **/
public class LocalDataOutputStream extends TISFSDataOutputStream {
    public LocalDataOutputStream(File f, boolean overwrite) throws IOException {
        super(new BufferedOutputStream(FileUtils.openOutputStream(f, !overwrite)));
    }

    @Override
    public void write(int b) throws IOException {
        this.out.write(b);
    }

    @Override
    public long getPos() throws IOException {
        throw new UnsupportedOperationException();
    }
}
