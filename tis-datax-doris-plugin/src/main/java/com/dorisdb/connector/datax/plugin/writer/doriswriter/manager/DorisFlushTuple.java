package com.dorisdb.connector.datax.plugin.writer.doriswriter.manager;

import java.util.List;

public class DorisFlushTuple {

    private String label;
    public final WriterBuffer buffer;

    public DorisFlushTuple(String label, WriterBuffer buffer) {
        this.label = label;
        this.buffer = buffer;
    }

    public String getLabel() {
        return label;
    }


    public List<byte[]> getRows() {
        return buffer.buffer;
    }

    public long getBytes() {
        return buffer.size;
    }
}