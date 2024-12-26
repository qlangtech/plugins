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

package com.qlangtech.tis.hive.reader.impl;


import com.google.common.collect.Sets;
import com.qlangtech.tis.config.hive.meta.HiveTable;
import com.qlangtech.tis.config.hive.meta.IHiveTableContext;
import com.qlangtech.tis.config.hive.meta.PartitionFilter;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.fs.IPath;
import com.qlangtech.tis.fullbuild.indexbuild.IDumpTable;
import com.qlangtech.tis.hive.DefaultHiveConnGetter;
import com.qlangtech.tis.hive.DefaultHiveTableContext;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;

import java.io.File;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-11-06 12:32
 **/
public class DefaultPartitionFilter extends PartitionFilter implements IdentityName {
    public static short maxPtsCount = (short) 999;
    /**
     * 不是必须输入的
     */
    @FormField(ordinal = 2, identity = true, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String ptFilter;

    @Override
    public String identityValue() {
        return ptFilter;
    }

    public static String getPtDftVal() {
//        DefaultPartitionFilter dftPartition = new DefaultPartitionFilter();
//        dftPartition.ptFilter =
//        return dftPartition;
        return IDumpTable.PARTITION_PT + " = " + HiveTable.KEY_PT_LATEST;
    }

    @Override
    public List<String> listStorePaths(IHiveTableContext hiveTableContext) {
        DefaultHiveTableContext tableContext = (DefaultHiveTableContext) hiveTableContext;
        Optional<String> filter = Optional.ofNullable(ptFilter);

        List<Partition> pts = null;
        if (filter.isPresent()) {

            String criteria = filter.get();
            if (StringUtils.indexOf(criteria, HiveTable.KEY_PT_LATEST) > -1) {

                criteria = DefaultHiveConnGetter.replaceLastestPtCriteria(criteria, (ptKey) -> {
                    int index = 0;
                    int matchedIndex = -1;
                    for (FieldSchema pt : tableContext.table.getPartitionKeys()) {
                        if (StringUtils.equals(ptKey, pt.getName())) {
                            matchedIndex = index;
                            break;
                        }
                        index++;
                    }

                    if (matchedIndex < 0) {
                        throw new IllegalStateException("has not find ptKey:" + ptKey + " in pt schema:" //
                                + tableContext.table.getPartitionKeys().stream().map((p) -> p.getName()).collect(Collectors.joining(",")));
                    }
                    Optional<String> maxPt = Optional.empty();
                    Set<String> latestPts = Sets.newHashSet();

                    for (Partition p : tableContext.listPartitions(maxPtsCount)) {
                        latestPts.add(p.getValues().get(matchedIndex));
                    }
                    maxPt = latestPts.stream().max((pt1, pt2) -> {
                        return pt1.compareTo(pt2);
                    });

                    return maxPt.orElseThrow(() -> new IllegalStateException("can not find maxPt latestPts.size()=" + latestPts.size()));
                });
            }

            pts = tableContext.listPartitionsByFilter(criteria, maxPtsCount);
        } else {
            pts = tableContext.listPartitions(maxPtsCount);
        }

        if (CollectionUtils.isEmpty(pts)) {
            throw new IllegalStateException("table:"
                    + tableContext.tableName + " must contain partition,for filter:" + filter.orElse("EMPTY"));
        }

//        List<String> result = pts.stream() //
//                .map((pt) -> IPath.pathConcat(tableContext.hiveTable.getStorageLocation(), pt)).collect(Collectors.toList());

        return pts.stream().map((pt) -> IPath.pathConcat(
                        tableContext.hiveTable.getStorageLocation()
                        , String.join(File.separator, pt.getValues())))
                .collect(Collectors.toList());

    }

    @TISExtension()
    public static final class DefaultDescriptor extends Descriptor<PartitionFilter> {
        @Override
        public String getDisplayName() {
            return SWITCH_ON;
        }

        public DefaultDescriptor() {
            super();
        }
    }

}
