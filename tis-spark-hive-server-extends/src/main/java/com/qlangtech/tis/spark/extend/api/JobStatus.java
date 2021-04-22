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
