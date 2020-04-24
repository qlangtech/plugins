package com.qlangtech.tis.fullbuild.indexbuild.impl;

import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;

import java.util.List;

/**
 * @author: baisui 百岁
 * @create: 2020-04-23 19:59
 **/
public class DefaultCallbackHandler implements AMRMClientAsync.CallbackHandler {
    private boolean isShutdown;

    public void onShutdownRequest() {
        System.out.println("onShutdownRequest");
        this.isShutdown = true;
    }

    public void onNodesUpdated(List<NodeReport> updatedNodes) {
        System.out.println("onNodesUpdated");
    }

    @Override
    public float getProgress() {
        return 0;
    }

    public void onError(Throwable e) {
        e.printStackTrace();
    }

    /* 由于这里没有slaver所以这里两个方法不会触发 */
    @Override
    public void onContainersCompleted(List<ContainerStatus> statuses) {
    }

    @Override
    public void onContainersAllocated(List<Container> containers) {
    }
}
