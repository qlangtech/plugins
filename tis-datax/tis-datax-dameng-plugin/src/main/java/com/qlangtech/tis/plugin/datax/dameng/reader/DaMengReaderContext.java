package com.qlangtech.tis.plugin.datax.dameng.reader;

import com.qlangtech.tis.plugin.datax.common.RdbmsReaderContext;
import com.qlangtech.tis.plugin.datax.dameng.ds.DaMengDataSourceFactory;
import com.qlangtech.tis.plugin.ds.IDataSourceDumper;
import com.qlangtech.tis.plugin.ds.SplitTableStrategy;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;

import java.util.List;
import java.util.Optional;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/9/15
 */
public class DaMengReaderContext extends RdbmsReaderContext<DataXDaMengReader, DaMengDataSourceFactory> {

    private final SplitTableStrategy splitTableStrategy;

    /**
     * @param jobName
     * @param sourceTableName 逻辑表表名
     * @param dumper
     * @param reader
     */
    public DaMengReaderContext(String jobName, String sourceTableName, IDataSourceDumper dumper, DataXDaMengReader reader) {
        super(jobName, sourceTableName, dumper, reader);
        this.splitTableStrategy = reader.getDataSourceFactory().splitTableStrategy;
    }

    public boolean isSplitTable() {
        // this.splitTableStrategy.getAllPhysicsTabs(this.dsFactory, this.getJdbcUrl(), this.sourceTableName);
        // return !(this.splitTableStrategy instanceof NoneSplitTableStrategy);
        return this.splitTableStrategy.isSplittable();
    }


    public String getTableWithEscape() {
        return EntityName.parse(this.sourceTableName).getFullName(Optional.of(colEscapeChar()));
    }

    public String getSplitTabs() {
        List<String> allPhysicsTabs = this.splitTableStrategy.getAllPhysicsTabs(dsFactory, this.getJdbcUrl(), this.sourceTableName);
        return getEntitiesWithQuotation(allPhysicsTabs);
    }

    public String getPassword() {
        return this.dsFactory.getPassword();
    }

    public String getUsername() {
        return this.dsFactory.getUserName();
    }

}
