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

package com.qlangtech.tis.plugin.datax;

import com.qlangtech.tis.plugin.ds.mangodb.MangoDBDataSourceFactory;

import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-20 20:43
 **/
public class BasicMongoDBContext {
    private final MangoDBDataSourceFactory dsFactory;

    public BasicMongoDBContext(MangoDBDataSourceFactory dsFactory) {
        this.dsFactory = dsFactory;
    }

    public String getServerAddress() {
        return MangoDBDataSourceFactory.getAddressList(this.dsFactory.address)
                .stream().map((address) -> "\"" + address + "\"").collect(Collectors.joining(","));
    }

    public boolean isContainCredential() {
        return this.dsFactory.isContainCredential();
    }

    public String getUserName() {
        return this.dsFactory.username;
    }

    public String getPassword() {
        return this.dsFactory.password;
    }

    public String getDbName() {
        return this.dsFactory.dbName;
    }
}
