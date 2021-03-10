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
