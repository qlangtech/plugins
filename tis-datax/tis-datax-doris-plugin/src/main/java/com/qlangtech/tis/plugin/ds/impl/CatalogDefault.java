package com.qlangtech.tis.plugin.ds.impl;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.ds.DataSourceCatalog;

/**
 * 默认的catalog ，在jdbc连接时候不使用特定的catalog值
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/4/13
 */
public class CatalogDefault extends DataSourceCatalog {
    @Override
    public void appendJdbcUrl(StringBuffer jdbcUrl, String dbName) {
        // super.appendJdbcUrl(jdbcUrl, dbName);
        jdbcUrl.append(dbName);
    }

    @Override
    public String getFullTableName(String dbName, String tableName) {
        return tableName;
    }

    @TISExtension
    public static class DefaultDesc extends Descriptor<DataSourceCatalog> {
        public DefaultDesc() {
            super();
        }

        @Override
        public String getDisplayName() {
            return SWITCH_DEFAULT;
        }
    }
}
