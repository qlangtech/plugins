package com.qlangtech.tis.plugin.datax;

import com.google.common.collect.Lists;

import java.util.List;

/**
 * @author: baisui 百岁
 * @create: 2021-04-08 13:29
 **/
public class SelectedTab {
    private final String name;
    // 是否选择所有的列
    private final boolean allCols;
    // 用户可以自己设置where条件
    private String where;

    public String getWhere() {
        return this.where;
    }

    public void setWhere(String where) {
        this.where = where;
    }

    private List<String> cols = Lists.newArrayList();

    public String getName() {
        return this.name;
    }

    public boolean isAllCols() {
        return this.allCols;
    }

    public List<String> getCols() {
        return this.cols;
    }

    public boolean containCol(String col) {
        return cols != null && this.cols.contains(col);
    }

    public void setCols(List<String> cols) {
        this.cols = cols;
    }

    public SelectedTab(String name, boolean allCols) {
        this.name = name;
        this.allCols = allCols;
    }
}
