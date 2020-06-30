package com.qlangtech.tis.fullbuild.indexbuild.impl;


import com.qlangtech.tis.plugin.IdentityName;

public interface IContainerPodSpec extends IdentityName {
//    // 最大yarn堆内存大小,单位为M
//    private final int maxYarnHeapMemory;
//    // 单个任务CPU内核大小
//     private final int maxYarnCPUCores;

    /**
     * 最大内存开销，单位：M兆
     *
     * @return
     */
    public int getMaxHeapMemory();

    /**
     * 最大内存core数
     *
     * @return
     */
    public int getMaxCPUCores();

    /**
     * 远程JAVA调试端口
     *
     * @return
     */
    public int getRunjdwpPort();

    /**
     * 最大容忍错误数量
     *
     * @return
     */
    default int getMaxMakeFaild() {
        return Integer.MAX_VALUE;
    }

}
