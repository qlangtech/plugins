package com.qlangtech.tis.plugin.ds.tidb;

import com.alibaba.citrus.turbine.Context;
import com.pingcap.tikv.TiConfiguration;
import com.pingcap.tikv.TiSession;
import com.pingcap.tikv.catalog.Catalog;
import com.pingcap.tikv.meta.TiDBInfo;
import com.pingcap.tikv.meta.TiTableInfo;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.*;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.sql.DataSource;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

/**
 * 针对PingCap TiKV作为数据源实现
 *
 * @author: baisui 百岁
 * @create: 2020-11-24 10:55
 **/
public class TiKVDataSourceFactory extends DataSourceFactory {

    private static final Logger logger = LoggerFactory.getLogger(TiKVDataSourceFactory.class);

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.host})
    public String pdAddrs;

    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
    public String dbName;

    @Override
    public DataDumpers getDataDumpers(TISTable table) {
        return null;
    }

    @Override
    public List<String> getTablesInDB() {
        TiConfiguration conf = TiConfiguration.createDefault(this.pdAddrs);
        TiSession session = TiSession.getInstance(conf);
        Catalog cat = session.getCatalog();
        TiDBInfo db = cat.getDatabase(dbName);
        List<TiTableInfo> tabs = cat.listTables(db);
        return tabs.stream().map((tt) -> tt.getName()).collect(Collectors.toList());
    }

    @Override
    public List<ColumnMetaData> getTableMetadata(String table) {
        throw new UnsupportedOperationException();
    }

    @Override
    public String getName() {
        return this.dbName;
    }


    @Override
    public DataSource createFacadeDataSource() {
        return null;
    }


    @TISExtension
    public static class DefaultDescriptor extends DataSourceFactory.BaseDataSourceFactoryDescriptor {

        @Override
        protected String getDataSourceName() {
            return "TiKV";
        }

        @Override
        protected boolean supportFacade() {
            return true;
        }

        @Override
        protected List<String> facadeSourceTypes() {
            return Collections.singletonList(DS_TYPE_MYSQL);
        }

        @Override
        protected boolean validate(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {

            try {
                ParseDescribable<DataSourceFactory> tikv = this.newInstance(postFormVals.rawFormData);
                DataSourceFactory sourceFactory = tikv.instance;
                List<String> tables = sourceFactory.getTablesInDB();
                if (tables.size() < 1) {
                    msgHandler.addErrorMessage(context, "TiKV库" + sourceFactory.getName() + "中的没有数据表");
                    return false;
                }
            } catch (Exception e) {
                msgHandler.addErrorMessage(context, e.getMessage());
                logger.warn(e.getMessage(), e);
                // throw new RuntimeException(e);
                return false;
            }
            return true;
        }
    }
}
