package com.qlangtech.tis.plugin.datax.dameng.writer;

import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.plugin.datax.dameng.RdbmsDataxContext;
import org.apache.commons.lang.StringUtils;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/9/15
 */
public class DaMengWriterContext extends RdbmsDataxContext implements IDataxContext {

    public DaMengWriterContext(String dataXName) {
        super(dataXName);
    }

    private String dbName;
    private String writeMode;
    private String preSql;
    private String postSql;
    private String session;
    private Integer batchSize;

    public void setDbName(String dbName) {
        this.dbName = dbName;
    }

    public void setWriteMode(String writeMode) {
        this.writeMode = writeMode;
    }

    public void setPreSql(String preSql) {
        this.preSql = preSql;
    }

    public void setPostSql(String postSql) {
        this.postSql = postSql;
    }

    public void setSession(String session) {
        this.session = session;
    }

    public void setBatchSize(Integer batchSize) {
        this.batchSize = batchSize;
    }

    public String getDbName() {
        return dbName;
    }

    public String getWriteMode() {
        return writeMode;
    }

    public String getPreSql() {
        return preSql;
    }

    public String getPostSql() {
        return postSql;
    }

    public String getSession() {
        return session;
    }

    public boolean isContainPreSql() {
        return StringUtils.isNotBlank(preSql);
    }

    public boolean isContainPostSql() {
        return StringUtils.isNotBlank(postSql);
    }

    public boolean isContainSession() {
        return StringUtils.isNotBlank(session);
    }

    public Integer getBatchSize() {
        return batchSize;
    }
}
