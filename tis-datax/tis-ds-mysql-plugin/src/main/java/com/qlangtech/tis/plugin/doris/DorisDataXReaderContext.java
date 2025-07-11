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

package com.qlangtech.tis.plugin.doris;

import com.qlangtech.tis.plugin.datax.RdbmsDataxContext;
import com.qlangtech.tis.plugin.datax.common.RdbmsReaderContext;
import com.qlangtech.tis.plugin.datax.common.RdbmsReaderContext.ISplitTableContext;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.SplitTableStrategy;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-07-04 05:52
 **/
public class DorisDataXReaderContext
        extends RdbmsReaderContext<DataXDorisReader, BasicDataSourceFactory> implements ISplitTableContext {
    private final RdbmsDataxContext rdbmsContext;
    private final SplitTableStrategy splitTableStrategy;

    public DorisDataXReaderContext(String name, String sourceTableName
            , RdbmsDataxContext mysqlContext, DataXDorisReader dataXReader) {
        super(name, sourceTableName, null, Objects.requireNonNull(dataXReader, "dataXReader can not be null"));
        this.rdbmsContext = mysqlContext;
        this.splitTableStrategy = Objects.requireNonNull(dsFactory.getSplitTableStrategy(), "splitTableStrategy can not be null");
    }

    /**
     * 是否执行分表导入
     *
     * @return
     */
    @Override
    public boolean isSplitTable() {
        // this.splitTableStrategy.getAllPhysicsTabs(this.dsFactory, this.getJdbcUrl(), this.sourceTableName);
        // return !(this.splitTableStrategy instanceof NoneSplitTableStrategy);
        return this.splitTableStrategy.isSplittable();
    }

    /**
     * 分表列表
     *
     * @return
     */
    @Override
    public String getSplitTabs() {
        List<String> allPhysicsTabs = this.splitTableStrategy.getAllPhysicsTabs(dsFactory, this.getJdbcUrl(), this.sourceTableName);
        return getEntitiesWithQuotation(allPhysicsTabs);
    }

    public String getDataXName() {
        return rdbmsContext.getDataXName();
    }

    public String getTabName() {
        return rdbmsContext.getTabName();
    }

    public String getPassword() {
        return rdbmsContext.getPassword();
    }

    public String getUsername() {
        return rdbmsContext.getUsername();
    }

    public String getJdbcUrl() {
        if (StringUtils.isEmpty(rdbmsContext.getJdbcUrl())) {
            throw new NullPointerException("rdbmsContext.getJdbcUrl() can not be empty");
        }
        return rdbmsContext.getJdbcUrl();
    }
}
