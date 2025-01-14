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

import com.beust.jcommander.internal.Lists;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.fs.IPath;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;

import java.io.File;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-01-13 11:32
 **/
public abstract class PartitionPathPattern implements Describable<PartitionPathPattern> {

    public final List<String> buildStoragePath(DefaultHiveTableContext tableContext, List<Partition> pts) {
        List<FieldSchema> ptKeys = tableContext.table.getPartitionKeys();
        return pts.stream().map((pt) -> {
                    List<String> ptPaths = this.createPTPaths(ptKeys,pt);  //Lists.newArrayList();
//                    List<String> ptVals = pt.getValues();
//                    for (int i = 0; i < ptKeys.size(); i++) {
                      //  ptPaths.add(ptKeys.get(i) + "=" + ptVals.get(i));
//                    }
                    return IPath.pathConcat(
                            tableContext.hiveTable.getStorageLocation()
                            , String.join(File.separator, ptPaths));
                })
                .collect(Collectors.toList());
    }


    protected abstract List<String> createPTPaths(List<FieldSchema> ptKeys, Partition pt);


}
