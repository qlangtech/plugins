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

package com.qlangtech.tis.plugin.ds.tidb;

import com.pingcap.tikv.meta.TiTableInfo;

/**
 * @author: baisui 百岁
 * @create: 2020-12-04 16:04
 **/
public class TiTableInfoWrapper {
    public final TiTableInfo tableInfo;
    boolean hasGetRowSize = false;

    public TiTableInfoWrapper(TiTableInfo tableInfo) {
        this.tableInfo = tableInfo;
    }

    /**
     * 如果有多个子任务获取rowsize则只有一个partition会返回整个table的rowSize，其他子任务返回0
     *
     * @return
     */
    public int getRowSize() {
        if (!hasGetRowSize) {
            hasGetRowSize = true;
            return (int) tableInfo.getEstimatedRowSizeInByte();
        }
        return 0;
    }
}
