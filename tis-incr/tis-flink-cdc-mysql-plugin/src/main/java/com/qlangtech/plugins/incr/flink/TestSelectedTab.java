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

package com.qlangtech.plugins.incr.flink;

import com.qlangtech.tis.plugin.ds.ISelectedTab;

import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-09-28 12:52
 **/
public class TestSelectedTab implements ISelectedTab {
    private final String tabName;
    private final List<ColMeta> cols;

    public TestSelectedTab(String tabName, List<ColMeta> cols) {
        this.tabName = tabName;
        this.cols = cols;
    }

    @Override
    public String getName() {
        return this.tabName;
    }

    @Override
    public String getWhere() {
        return null;
    }

    @Override
    public boolean isAllCols() {
        return false;
    }

    @Override
    public List<ColMeta> getCols() {
        return cols;
    }
}
