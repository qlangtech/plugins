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

import com.qlangtech.tis.config.hive.meta.IHiveTableContext;
import com.qlangtech.tis.config.hive.meta.PartitionFilter;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.fs.IPath;
import com.qlangtech.tis.hive.DefaultHiveTableContext;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.hive.metastore.api.Partition;

import java.util.Collections;
import java.util.List;

/**
 * 表没有分区
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-11-06 12:31
 **/
public class NonePartition extends PartitionFilter {

    public static final PartitionFilter create() {
        return new NonePartition();
    }

    @Override
    public List<String> listStorePaths(IHiveTableContext hiveTableContext) {
        final DefaultHiveTableContext tableContext = (DefaultHiveTableContext) hiveTableContext;
        List<Partition> pts = tableContext.listPartitions((short) 1);
        // 不能拥有分区
        if (CollectionUtils.isNotEmpty(pts)) {
            throw new IllegalStateException("can not contain partitions for table:" + tableContext.tableName);
        }
        return Collections.singletonList(IPath.pathConcat(tableContext.hiveTable.getStorageLocation()));
    }

    @TISExtension()
    public static final class DefaultDescriptor extends Descriptor<PartitionFilter> {
        @Override
        public String getDisplayName() {
            return SWITCH_OFF;
        }

        public DefaultDescriptor() {
            super();
        }
    }
}
