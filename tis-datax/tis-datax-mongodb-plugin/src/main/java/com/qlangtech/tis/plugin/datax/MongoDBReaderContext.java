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

import com.qlangtech.tis.datax.IDataxReaderContext;
import com.qlangtech.tis.plugin.datax.common.RdbmsReaderContext;
import com.qlangtech.tis.plugin.datax.mongo.MongoSelectedTab;
import com.qlangtech.tis.plugin.datax.mongo.MongoSelectedTabExtend;
import com.qlangtech.tis.plugin.ds.IDataSourceDumper;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-06 14:53
 **/
public class MongoDBReaderContext extends RdbmsReaderContext implements IDataxReaderContext {

    private final MongoSelectedTab mongoTable;
    private final MongoSelectedTabExtend tabExtend;

    public MongoDBReaderContext(String jobName, SelectedTab tab, IDataSourceDumper dumper,
                                DataXMongodbReader mongodbReader) {
        super(jobName, tab.getName(), dumper, mongodbReader);
        this.mongoTable = (MongoSelectedTab) tab;
        this.tabExtend = (MongoSelectedTabExtend) this.mongoTable.getSourceProps();
    }


    public String getColumn() {
        // return this.mongodbReader.column;
        return null;
    }

    public boolean isContainQuery() {
        //  return StringUtils.isNotEmpty(this.mongodbReader.query);
        return true;
    }

    public String getQuery() {
        //  return this.mongodbReader.query;
        return null;
    }


    @Override
    public String getSourceTableName() {
        return DataXMongodbReader.DATAX_NAME;
    }

    @Override
    public String getSourceEntityName() {
        return DataXMongodbReader.DATAX_NAME;
    }
}
