package com.qlangtech.tis.plugin.ds.impl;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.DataSourceCatalog;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/4/13
 */
public class CatalogSpecific extends DataSourceCatalog {
    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.db_col_name})
    public String name;

    @Override
    public void appendJdbcUrl(StringBuffer jdbcUrl, String dbName) {
        jdbcUrl.append(name).append(".").append(dbName);
    }

    @Override
    public String getFullTableName(String dbName, String tableName) {
        return this.name + "." + dbName + "." + tableName;
    }

    @TISExtension
    public static class DefaultDesc extends Descriptor<DataSourceCatalog> {
        public DefaultDesc() {
            super();
        }

        @Override
        public String getDisplayName() {
            return SWITCH_CUSTOMIZE;
        }
    }
}
