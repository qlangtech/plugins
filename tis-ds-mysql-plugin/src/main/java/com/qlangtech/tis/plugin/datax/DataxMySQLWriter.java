package com.qlangtech.tis.plugin.datax;

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.DataSourceFactoryPluginStore;
import com.qlangtech.tis.plugin.ds.PostedDSProp;

/**
 * @author: baisui 百岁
 * @create: 2021-04-07 15:30
 **/
public class DataxMySQLWriter extends DataxWriter {
    private static final String DATAX_NAME = "MySQL";
    public static final String KEY_DB_NAME_FIELD_NAME = "dbName";

    // @FormField(identity = true, ordinal = 0, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String dbName;

    @Override
    public String getWriterName() {
        return "mysqlwriter";
    }

    @Override
    public String getTemplate() {
        return null;
    }

    @Override
    public IDataxContext getSubTask() {
        return new MySQLDataxContext();
    }

    private DataSourceFactory getDataSourceFactory() {
        DataSourceFactoryPluginStore dsStore = TIS.getDataBasePluginStore(new PostedDSProp(this.dbName));
        return dsStore.getPlugin();
    }

    @TISExtension()
    public static class DefaultDescriptor extends Descriptor<DataxWriter> {
        public DefaultDescriptor() {
            super();
//            this.registerSelectOptions(KEY_DB_NAME_FIELD_NAME, () -> {
//                List<DataSourceFactory> allDbs = DataSourceFactory.all();
//                return allDbs.stream().filter((db) -> db instanceof MySQLDataSourceFactory).collect(Collectors.toList());
//            });
        }

        @Override
        public String getDisplayName() {
            return DATAX_NAME;
        }
    }
}
