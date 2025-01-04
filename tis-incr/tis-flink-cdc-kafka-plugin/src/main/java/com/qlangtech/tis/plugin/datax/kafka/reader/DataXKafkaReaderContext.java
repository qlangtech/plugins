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

package com.qlangtech.tis.plugin.datax.kafka.reader;

import com.qlangtech.tis.datax.IDataxReaderContext;
import com.qlangtech.tis.plugin.datax.SelectedTab;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-01-02 14:21
 **/
public class DataXKafkaReaderContext implements IDataxReaderContext {

    private final SelectedTab kafkaTab;
    private final String jobName;

    public DataXKafkaReaderContext(String jobName, SelectedTab kafkaTab) {
        this.kafkaTab = kafkaTab;
        this.jobName = jobName;
    }

    @Override
    public String getReaderContextId() {
        throw new UnsupportedOperationException("getReaderContextId not support ");
    }

    @Override
    public String getTaskName() {
        return jobName;
    }

    @Override
    public String getSourceEntityName() {
        return kafkaTab.getName();
    }

    @Override
    public String getSourceTableName() {
        return kafkaTab.getName();
    }
}
