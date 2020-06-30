package com.qlangtech.tis.spark.extend.api;

/**
 * @author: baisui 百岁
 * @create: 2020-06-09 15:53
 **/
public class JobStatus {

    private int allTaskCount;
    private int completeTaskCount;

    public JobStatus() {
    }

    public JobStatus(int allTaskCount, int completeTaskCount) {
        this.allTaskCount = allTaskCount;
        this.completeTaskCount = completeTaskCount;
    }

    public void addFinishedTask(int count) {
        this.completeTaskCount += count;
    }

    public int executePercent() {
        return completeTaskCount * 100 / (allTaskCount);
    }

    public int getAllTaskCount() {
        return allTaskCount;
    }

    public int getCompleteTaskCount() {
        return completeTaskCount;
    }

}
