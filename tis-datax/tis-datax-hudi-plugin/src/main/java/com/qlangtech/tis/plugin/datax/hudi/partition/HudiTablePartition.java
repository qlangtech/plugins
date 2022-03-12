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

package com.qlangtech.tis.plugin.datax.hudi.partition;

import com.alibaba.datax.plugin.writer.hudi.TypedPropertiesBuilder;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.plugin.datax.CreateTableSqlBuilder;
import com.qlangtech.tis.plugin.datax.hudi.DataXHudiWriter;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-03-05 10:58
 **/
@Public
public abstract class HudiTablePartition implements Describable<HudiTablePartition> {

    protected static void setPartitionProps(TypedPropertiesBuilder props, String field, String partition_extractor_class) {
        props.setProperty("hoodie.datasource.hive_sync.partition_fields", field);
        // "org.apache.hudi.hive.MultiPartKeysValueExtractor";
        // partition 分区值抽取类
        props.setProperty("hoodie.datasource.hive_sync.partition_extractor_class", partition_extractor_class);
    }

    protected static void appendPartitionsOnSQLDDL(List<String> pts, CreateTableSqlBuilder createTableSqlBuilder) {
        createTableSqlBuilder.script.appendLine("\t,");
        createTableSqlBuilder.appendColName(pts.stream().collect(Collectors.joining(",")));
        createTableSqlBuilder.script
                .append("VARCHAR(30)")
                .returnLine();
    }


    public void setProps(TypedPropertiesBuilder props, DataXHudiWriter hudiWriter) {
        props.setProperty("hoodie.datasource.write.partitionpath.field", hudiWriter.partitionedBy);
    }

    public boolean isSupportPartition() {
        return true;
    }

    public abstract void addPartitionsOnSQLDDL(List<String> pts, CreateTableSqlBuilder createTableSqlBuilder);
}
