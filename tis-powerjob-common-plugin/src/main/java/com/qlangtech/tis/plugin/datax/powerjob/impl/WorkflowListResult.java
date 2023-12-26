package com.qlangtech.tis.plugin.datax.powerjob.impl;

import java.util.List;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/11/27
 */
public class WorkflowListResult {

    private int index;
    private int pageSize;
    private int totalPages;
    private int totalItems;

    private List<TISWorkflowInfoDTO> data;

    public List<TISWorkflowInfoDTO> getData() {
        return data;
    }

    public void setData(List<TISWorkflowInfoDTO> data) {
        this.data = data;
    }

    public int getIndex() {
        return index;
    }

    public void setIndex(int index) {
        this.index = index;
    }

    public int getPageSize() {
        return pageSize;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }

    public int getTotalPages() {
        return totalPages;
    }

    public void setTotalPages(int totalPages) {
        this.totalPages = totalPages;
    }

    public int getTotalItems() {
        return totalItems;
    }

    public void setTotalItems(int totalItems) {
        this.totalItems = totalItems;
    }
}
