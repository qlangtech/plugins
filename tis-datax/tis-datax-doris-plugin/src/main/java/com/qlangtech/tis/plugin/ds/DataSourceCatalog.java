package com.qlangtech.tis.plugin.ds;

import com.qlangtech.tis.extension.Describable;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/4/13
 */
public abstract class DataSourceCatalog implements Describable<DataSourceCatalog> {
    public abstract void appendJdbcUrl(StringBuffer jdbcUrl, String dbName);
//    {
//        jdbcUrl.append(dbName);
//    }

    public abstract String getFullTableName(String dbName, String tableName);


}
