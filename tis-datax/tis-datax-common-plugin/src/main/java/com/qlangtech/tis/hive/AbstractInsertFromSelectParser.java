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

package com.qlangtech.tis.hive;

import com.qlangtech.tis.fullbuild.indexbuild.IDumpTable;
import com.qlangtech.tis.plugin.ds.ColMeta;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.sql.parser.ISqlTask;
import com.qlangtech.tis.sql.parser.SqlTaskNodeMeta;
import com.qlangtech.tis.sql.parser.TabPartitions;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-02-25 09:02
 **/
public abstract class AbstractInsertFromSelectParser {
    private final List<HiveColumn> cols = new ArrayList<>();

    public List<HiveColumn> getCols() {
        return cols;
    }

    /**
     * 除去ps列
     */
    public List<HiveColumn> getColsExcludePartitionCols() {
        return getCols().stream().filter((r) -> !IDumpTable.preservedPsCols.contains(r.getName())).collect(Collectors.toList());
    }

    /**
     * @param sql
     * @param tabPartition
     */
    public void start(String sql, TabPartitions tabPartition
            , Function<ISqlTask.RewriteSql, List<ColumnMetaData>> sqlColMetaGetter) {

        SqlTaskNodeMeta sqlTaskNodeMeta = new SqlTaskNodeMeta();
        sqlTaskNodeMeta.setSql(sql);
        ISqlTask.RewriteSql rewriteSql = sqlTaskNodeMeta.getColMetaGetterSql(tabPartition);

        HiveColumn hc = null;
        int index = 0;

        for (ColMeta col : rewriteSql.getCols()) {
            hc = new HiveColumn();
            hc.setName(col.getName());
            hc.setDataType(col.type);
            hc.setIndex(index++);
            this.cols.add(hc);
        }
        List<ColumnMetaData> colsMeta = sqlColMetaGetter.apply(rewriteSql);
        List<HiveColumn> allCols = this.getCols();
        int allColSize = allCols.size();
        if (allColSize != colsMeta.size()) {
            throw new IllegalStateException("cols.size():" + allColSize + ",colsMeta.size():" + colsMeta.size() + " is not equal");
        }

        Map<String, HiveColumn> colsMapper = allCols.stream().collect(Collectors.toMap((c) -> c.getName(), (c) -> c));


        colsMeta.forEach((c) -> Objects.requireNonNull(
                colsMapper.get(c.getKey()), "col:" + c.getKey() + " can not find in colsMapper").setDataType(c.getType()));

    }

}
