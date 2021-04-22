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

import com.qlangtech.tis.fs.FSDataInputStream;
import org.apache.commons.io.IOUtils;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * @author: baisui 百岁
 * @create: 2021-03-02 13:44
 **/
public class LocalFSDataInputStream extends FSDataInputStream {

    public LocalFSDataInputStream(InputStream input, int bufferSize) {
        super((new BufferedInputStream(input, bufferSize)));
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
        IOUtils.read(this, buffer, offset, length);
    }

    @Override
    public void seek(long position) {
        throw new UnsupportedOperationException();
    }


}
