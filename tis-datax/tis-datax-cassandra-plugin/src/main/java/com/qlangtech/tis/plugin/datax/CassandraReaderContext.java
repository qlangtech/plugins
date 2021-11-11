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

import com.google.common.collect.Lists;
import com.qlangtech.tis.plugin.datax.common.RdbmsReaderContext;
import com.qlangtech.tis.plugin.ds.IDataSourceDumper;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.cassandra.CassandraDatasourceFactory;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-06 15:18
 **/
public class CassandraReaderContext extends RdbmsReaderContext<DataXCassandraReader, CassandraDatasourceFactory> {

    private final SelectedTab tab;

    public CassandraReaderContext(String jobName, SelectedTab tab, IDataSourceDumper dumper, DataXCassandraReader reader) {
        super(jobName, tab.getName(), dumper, reader);
        this.tab = tab;
    }

    public boolean isContainAllowFiltering() {
        return this.plugin.allowFiltering != null;
    }

    public boolean isAllowFiltering() {
        return this.plugin.allowFiltering;
    }

    public boolean isContainConsistancyLevel() {
        return StringUtils.isNotEmpty(this.plugin.consistancyLevel);
    }

    public String getConsistancyLevel() {
        return this.plugin.consistancyLevel;
    }

    public String getTable() {
        return this.tab.getName();
    }

    public boolean isContainWhere() {
        return StringUtils.isNotEmpty(this.tab.where);
    }

    public String getWhere() {
        return this.tab.where;
    }

    public List<ISelectedTab.ColMeta> getColumn() {
        return this.tab.getCols();
    }

    public String getKeyspace() {
        return this.dsFactory.dbName;
    }

    public boolean isSupportUseSSL() {
        return this.dsFactory.useSSL != null;
    }

    public boolean isUseSSL() {
        return this.dsFactory.useSSL;
    }

    public String getHost() {
        return Lists.newArrayList(this.dsFactory.getHosts()).stream().collect(Collectors.joining(","));
    }

    public int getPort() {
        return this.dsFactory.port;
    }

    public boolean isContainUsername() {
        return StringUtils.isNotEmpty(this.dsFactory.userName);
    }

    public boolean isContainPassword() {
        return StringUtils.isNotEmpty(this.dsFactory.password);
    }

    public String getUsername() {
        return this.dsFactory.userName;
    }

    public String getPassword() {
        return this.dsFactory.password;
    }
}
