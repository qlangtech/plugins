package com.qlangtech.tis.plugin.datax;

import com.qlangtech.tis.datax.DataXJobInfo;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/11/11
 */
public class SplitTabInfo {
    private final int taskSerializeNum;
    private final DataXJobInfo dataXInfo;

    public SplitTabInfo(int taskSerializeNum, DataXJobInfo dataXInfo) {
        this.taskSerializeNum = taskSerializeNum;
        this.dataXInfo = dataXInfo;
    }

    public int getTaskSerializeNum() {
        return taskSerializeNum;
    }

    public String getDataXInfo() {
        return dataXInfo.serialize();
    }
}
