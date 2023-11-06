package com.qlangtech.tis.plugin.datax.powerjob.impl.oms;

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.powerjob.PowerJobOMSStorage;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.PostedDSProp;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/10/31
 */
public class MySQLPowerJobOMSStorage extends PowerJobOMSStorage {
    @FormField(ordinal = 0, type = FormFieldType.ENUM, validate = {Validator.require})
    public String dbName;

    public DataSourceFactory getDataSourceFactory() {
        return TIS.getDataBasePlugin(PostedDSProp.parse(this.dbName));
    }
    @TISExtension
    public static class DefaultDesc extends Descriptor<PowerJobOMSStorage> {
        @Override
        public String getDisplayName() {
            return "MySQL";
        }
    }
}