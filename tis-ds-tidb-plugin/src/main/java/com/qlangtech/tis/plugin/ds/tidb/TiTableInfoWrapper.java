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
