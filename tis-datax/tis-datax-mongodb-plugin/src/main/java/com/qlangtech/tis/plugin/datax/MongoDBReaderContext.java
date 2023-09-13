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

import com.alibaba.datax.common.element.Column;
import com.mongodb.Function;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReaderContext;
import com.qlangtech.tis.datax.TableAlias;
import com.qlangtech.tis.plugin.datax.common.RdbmsReaderContext;
import com.qlangtech.tis.plugin.datax.mongo.MongoCMeta;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.ds.IDataSourceDumper;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.mangodb.MangoDBDataSourceFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.bson.Document;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-06 14:53
 **/
public class MongoDBReaderContext extends RdbmsReaderContext<DataXMongodbReader, MangoDBDataSourceFactory> implements IDataxReaderContext {

    final SelectedTab mongoTable;
    //  private final MongoSelectedTabExtend tabExtend;

    public MongoDBReaderContext(String jobName, SelectedTab tab, IDataSourceDumper dumper,
                                DataXMongodbReader mongodbReader) {
        super(jobName, tab.getName(), dumper, mongodbReader);
        this.mongoTable = tab;
        //    this.tabExtend = (MongoSelectedTabExtend) this.mongoTable.getSourceProps();
        //

        //  this.setCols(cols.stream().map((c) -> c.getKey().getName()).collect(Collectors.toList()));
    }


    @Override
    public IDataxProcessor.TableMap createTableMap(TableAlias tableAlias, ISelectedTab selectedTab) {
        if (!StringUtils.equals(this.mongoTable.getName(), selectedTab.getName())) {
            throw new IllegalStateException("this.mongoTable.getName():" + this.mongoTable.getName()//
                    + ",selectedTab" + ".getName():" + selectedTab.getName() + " shall be equal");
        }
        return new IDataxProcessor.TableMap(tableAlias, selectedTab) {
            @Override
            public List<CMeta> getSourceCols() {
                DataXMongodbReader.DefaultMongoTable mtable =
                        (DataXMongodbReader.DefaultMongoTable) ((DataXMongodbReader) plugin).findMongoTable(selectedTab.getName());
                List<Pair<MongoCMeta, Function<Document, Column>>> cols = mtable.getMongoPresentCols();
                return cols.stream().map((p) -> p.getKey()).collect(Collectors.toList());
            }
        };
    }

    public String getDataSourceFactoryId() {
        return Objects.requireNonNull(dsFactory.identityValue(), "dataSourceId factory id can not be empty");//
    }

    public String getDbName() {
        return Objects.requireNonNull(dsFactory, "dbFactory can not be null").dbName;
    }

    @Override
    public String getSourceTableName() {
        return this.mongoTable.getName();
    }

    @Override
    public String getSourceEntityName() {
        return this.mongoTable.getName();
    }
}
