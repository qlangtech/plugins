package com.qlangtech.tis.plugin.ds.mysql;

import com.qlangtech.tis.plugin.ds.DBConfig;
import org.springframework.beans.BeansException;

/**
 * @author: baisui 百岁
 * @create: 2020-12-08 13:52
 **/
public abstract class DBRegister {

    // 由于facade的dbname会和detail的不一样，所以需要额外在加一个供注册到spring datasource中作为id用
    private final String dbName;

    private final DBConfig dbConfig;

    public DBRegister(String dbName, DBConfig dbConfig) {
        this.dbName = dbName;
        this.dbConfig = dbConfig;
    }

    protected abstract void createDefinition(String dbDefinitionId, String driverClassName, String jdbcUrl, String userName, String password);

    /**
     * 读取多个数据源中的一个一般是用于读取数据源Meta信息用
     *
     * @throws BeansException
     */
    public void visitFirst() throws BeansException {
        this.setApplicationContext(false, true);
    }

    /**
     * 读取所有可访问的数据源
     */
    public void visitAll() {
        this.setApplicationContext(true, false);
    }

    private void setApplicationContext(boolean resolveHostIp, boolean facade) throws BeansException {
        this.dbConfig.vistDbURL(resolveHostIp, (dbName, jdbcUrl) -> {
            final String dbDefinitionId = (facade ? DBRegister.this.dbName : dbName);
            createDefinition(dbDefinitionId, "com.mysql.jdbc.Driver", jdbcUrl, dbConfig.getUserName(), dbConfig.getPassword());
        }, facade);
    }
}
