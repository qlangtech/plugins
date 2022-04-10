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

package com.alibaba.datax.plugin.writer.hudi;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-03-23 14:48
 **/
@FunctionalInterface
public interface IPropertiesBuilder {
    String KEY_HOODIE_PARTITIONPATH_FIELD = "hoodie.datasource.write.partitionpath.field";
    String KEY_HOODIE_DATASOURCE_WRITE_KEYGENERATOR_TYPE = "hoodie.datasource.write.keygenerator.type";
    String KEY_HOODIE_DATASOURCE_HIVE_SYNC_PARTITION_FIELDS = "hoodie.datasource.hive_sync.partition_fields";
    String KEY_HOODIE_DATASOURCE_HIVE_SYNC_PARTITION_EXTRACTOR_CLASS = "hoodie.datasource.hive_sync.partition_extractor_class";

    public void setProperty(String key, String value);
}
