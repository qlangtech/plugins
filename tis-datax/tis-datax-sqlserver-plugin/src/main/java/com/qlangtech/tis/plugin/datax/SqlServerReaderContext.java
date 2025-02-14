/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qlangtech.tis.plugin.datax;

import com.qlangtech.tis.plugin.datax.common.RdbmsReaderContext;
import com.qlangtech.tis.plugin.datax.common.RdbmsReaderContext.ISplitTableContext;
import com.qlangtech.tis.plugin.ds.IDataSourceDumper;
import com.qlangtech.tis.plugin.ds.SplitTableStrategy;
import com.qlangtech.tis.plugin.ds.sqlserver.SqlServerDatasourceFactory;

import java.util.List;
import java.util.Objects;


/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-06 14:59
 **/
public class SqlServerReaderContext
        extends RdbmsReaderContext<DataXSqlserverReader, SqlServerDatasourceFactory> implements ISplitTableContext {
    private final SplitTableStrategy splitTableStrategy;

    public SqlServerReaderContext(String jobName
            , String sourceTableName, IDataSourceDumper dumper, DataXSqlserverReader reader) {
        super(jobName, sourceTableName, dumper, reader);
        this.splitTableStrategy = Objects.requireNonNull(dsFactory.splitTableStrategy, "splitTableStrategy can not be null");
    }

    @Override
    protected String colEscapeChar() {
        return SqlServerWriterContext.EscapeChar;
    }

    public String getUserName() {
        return dsFactory.getUserName();
    }

    public String getPassword() {
        return dsFactory.getPassword();
    }

    public boolean isContainSplitPk() {
        return this.plugin.splitPk != null;
    }

    public boolean isSplitPk() {
        return this.plugin.splitPk;
    }

    public boolean isContainFetchSize() {
        return this.plugin.fetchSize != null;
    }

    public int getFetchSize() {
        return this.plugin.fetchSize;
    }


    @Override
    public boolean isSplitTable() {
        return splitTableStrategy.isSplittable();
    }

    @Override
    public String getSplitTabs() {
        List<String> allPhysicsTabs = this.splitTableStrategy.getAllPhysicsTabs(dsFactory, this.getJdbcUrl(), this.sourceTableName);
        return getEntitiesWithQuotation(false, allPhysicsTabs);
    }

    public static void main(String[] args) {


    }
}
