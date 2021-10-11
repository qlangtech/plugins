/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.tis.plugin.ds;

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

    protected abstract void createDefinition(String dbDefinitionId, String driverClassName, String jdbcUrl);

    /**
     * 读取多个数据源中的一个一般是用于读取数据源Meta信息用
     */
    public void visitFirst() {
        this.setApplicationContext(false, true);
    }

    /**
     * 读取所有可访问的数据源
     */
    public void visitAll() {
        this.setApplicationContext(true, false);
    }

    private void setApplicationContext(boolean resolveHostIp, boolean facade) {
        this.dbConfig.vistDbURL(resolveHostIp, (dbName, jdbcUrl) -> {
            final String dbDefinitionId = (facade ? DBRegister.this.dbName : dbName);
            createDefinition(dbDefinitionId, "com.mysql.jdbc.Driver", jdbcUrl);
        }, facade);
    }
}
