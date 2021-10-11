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
