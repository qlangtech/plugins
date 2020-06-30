package com.qlangtech.tis.dump;

public interface IExecLiveLogParser {
    void process(String log);

    boolean isExecOver();
}
