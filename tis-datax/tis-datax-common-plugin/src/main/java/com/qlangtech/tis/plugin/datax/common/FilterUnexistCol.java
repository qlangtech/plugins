package com.qlangtech.tis.plugin.datax.common;

import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/9/11
 */
public interface FilterUnexistCol extends AutoCloseable {

    static FilterUnexistCol noneFilter() {
        return new FilterUnexistCol() {
            @Override
            public List<String> getCols(SelectedTab tab) {
                return tab.getColKeys();
            }

            @Override
            public void close() throws Exception {

            }
        };
    }

    static FilterUnexistCol filterByRealtimeSchema(BasicDataXRdbmsReader rdbmsReader) {
        final TableColsMeta tabColsMap = rdbmsReader.getTabsMeta();
        return new FilterUnexistCol() {
            @Override
            public List<String> getCols(SelectedTab tab) {
                Map<String, ColumnMetaData> tableMetadata = tabColsMap.get(tab.getName());
                return tab.getColKeys().stream().filter((c) -> tableMetadata.containsKey(c)).collect(Collectors.toList());
            }

            @Override
            public void close() throws Exception {
                tabColsMap.close();
            }
        };
    }

    public List<String> getCols(SelectedTab tab);
}
