package com.qlangtech.tis.fs.local;

import com.google.common.io.CountingInputStream;
import com.qlangtech.tis.fs.FSDataInputStream;
import org.apache.commons.io.IOUtils;

import java.io.IOException;
import java.io.InputStream;

/**
 * @author: baisui 百岁
 * @create: 2021-03-02 13:44
 **/
public class LocalFSDataInputStream extends FSDataInputStream {

    private final CountingInputStream pos;

    public LocalFSDataInputStream(InputStream input, int bufferSize) {
        // super(new CountingInputStream(new BufferedInputStream(input, bufferSize)));
        super(new CountingInputStream(input));

        this.pos = (CountingInputStream) in;
    }

    @Override
    public void readFully(long position, byte[] buffer, int offset, int length) throws IOException {
        IOUtils.read(this, buffer, offset, length);
    }

    @Override
    public void seek(long position) {
        try {
            long count = pos.getCount();
            if (position > 0 && position > count) {
                this.skip(position - count);
                //this.in.skip(position - p);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


}
