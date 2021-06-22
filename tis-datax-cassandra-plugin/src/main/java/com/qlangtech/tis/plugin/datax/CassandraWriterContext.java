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
import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.ISelectedTab;
import com.qlangtech.tis.plugin.ds.cassandra.CassandraDatasourceFactory;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-21 18:18
 **/
public class CassandraWriterContext implements IDataxContext {
    private final DataXCassandraWriter writer;
    private final IDataxProcessor.TableMap tabMapper;
    private final CassandraDatasourceFactory dsFactory;

    public CassandraWriterContext(DataXCassandraWriter writer, IDataxProcessor.TableMap tabMapper) {
        this.writer = writer;
        this.tabMapper = tabMapper;
        this.dsFactory = writer.getDataSourceFactory();
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

    public String getTable() {
        return this.tabMapper.getTo();
    }

    public List<ISelectedTab.ColMeta> getColumn() {
        return this.tabMapper.getSourceCols();
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

    public boolean isContainConnectionsPerHost() {
        return this.writer.connectionsPerHost != null && this.writer.connectionsPerHost > 0;
    }

    public int getConnectionsPerHost() {
        return this.writer.connectionsPerHost;
    }

    public boolean isContainMaxPendingPerConnection() {
        return this.writer.maxPendingPerConnection != null && this.writer.maxPendingPerConnection > 0;
    }

    public int getMaxPendingPerConnection() {

        return this.writer.maxPendingPerConnection;
    }

    public boolean isContainConsistancyLevel() {
        return StringUtils.isNotBlank(this.writer.consistancyLevel);
    }

    public String getConsistancyLevel() {
        return this.writer.consistancyLevel;
    }

    public boolean isContainBatchSize() {
        return this.writer.batchSize != null && this.writer.batchSize > 0;
    }

    public int getBatchSize() {
        return this.writer.batchSize;
    }
}
