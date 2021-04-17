package com.qlangtech.tis.plugin.datax;

import com.google.common.collect.Lists;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

import java.util.List;

/**
 * @author: baisui 百岁
 * @create: 2021-04-08 13:29
 **/
public class SelectedTab {
    // 表名称
    @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String name;
    // 是否选择所有的列
    // public boolean allCols;
    // 用户可以自己设置where条件
    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT)
    public String where;

    @FormField(ordinal = 2, type = FormFieldType.MULTI_SELECTABLE, validate = {Validator.require})
    public List<String> cols = Lists.newArrayList();

    public String getWhere() {
        return this.where;
    }


    public void setWhere(String where) {
        this.where = where;
    }

    public String getName() {
        return this.name;
    }

    public boolean isAllCols() {
        return this.cols.isEmpty();
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

    public SelectedTab(String name) {
        this.name = name;
    }

    public SelectedTab() {
    }
}
