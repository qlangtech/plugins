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

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.hive.PartitionPathPattern;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Partition;

import java.util.List;

/**
 * /user/hive/warehouse/sales_data/2023/1
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-01-13 11:34
 **/
public class PartitionPathPatternWithoutPtKeys extends PartitionPathPattern {

    @Override
    protected List<String> createPTPaths(List<FieldSchema> ptKeys, Partition pt) {
        return pt.getValues();
    }

    @TISExtension
    public static class Desc extends Descriptor<PartitionPathPattern> {
        @Override
        public String getDisplayName() {
            return "WithoutPtKeys";
        }
    }
}
