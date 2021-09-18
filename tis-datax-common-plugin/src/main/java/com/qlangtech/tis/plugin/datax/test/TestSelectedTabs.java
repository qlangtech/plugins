/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.tis.plugin.datax.test;

import com.google.common.collect.Lists;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.ISelectedTab;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;

import java.sql.Types;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * 测试用的mock对象
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-23 10:18
 **/
public class TestSelectedTabs {
    public static final String tabNameOrderDetail = "orderdetail";
    public static final String tabNameTotalpayinfo = "totalpayinfo";
    public static List<ColumnMetaData> tabColsMetaOrderDetail
            = Lists.newArrayList(new ColumnMetaData(0, "col1", Types.VARCHAR, true)
            , new ColumnMetaData(1, "col2", Types.VARCHAR, false)
            , new ColumnMetaData(2, "col3", Types.VARCHAR, false)
            , new ColumnMetaData(3, "col4", Types.VARCHAR, false)
    );
    public static List<ColumnMetaData> tabColsMetaTotalpayinfo
            = Lists.newArrayList(new ColumnMetaData(0, "col1", Types.VARCHAR, true)
            , new ColumnMetaData(1, "col2", Types.VARCHAR, false)
            , new ColumnMetaData(2, "col3", Types.VARCHAR, false)
            , new ColumnMetaData(3, "col4", Types.VARCHAR, false)
    );

    public static List<SelectedTab> createSelectedTabs() {
        return createSelectedTabs(Integer.MAX_VALUE);
    }

    public static List<SelectedTab> createSelectedTabs(int count) {
        List<SelectedTab> selectedTabs = Lists.newArrayList();
        SelectedTab selectedTab = new SelectedTab();
        selectedTab.setCols(Lists.newArrayList("col1", "col2", "col3"));
        selectedTab.setWhere("delete = 0");
        selectedTab.name = tabNameOrderDetail;
        selectedTabs.add(selectedTab);

        if (count > 1) {
            selectedTab = new SelectedTab();
            selectedTab.setCols(Lists.newArrayList("col1", "col2", "col3", "col4"));
            selectedTab.setWhere("delete = 0");
            selectedTab.name = tabNameTotalpayinfo;
            selectedTabs.add(selectedTab);
        }
        return selectedTabs;
    }

    public static Optional<IDataxProcessor.TableMap> createTableMapper() {
        IDataxProcessor.TableMap tm = new IDataxProcessor.TableMap();
        tm.setFrom("orderinfo");
        tm.setTo("orderinfo_new");
        tm.setSourceCols(Lists.newArrayList("col1", "col2", "col3").stream().map((c) -> {
            ISelectedTab.ColMeta meta = new ISelectedTab.ColMeta();
            meta.setName(c);
            meta.setType(ISelectedTab.DataXReaderColType.STRING);
            return meta;
        }).collect(Collectors.toList()));
        Optional<IDataxProcessor.TableMap> tableMap = Optional.of(tm);
        return tableMap;
    }
}
