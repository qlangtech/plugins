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
