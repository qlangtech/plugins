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

import com.google.common.collect.Lists;
import com.qlangtech.tis.datax.DBDataXChildTask;
import com.qlangtech.tis.datax.IDataxReaderContext;
import com.qlangtech.tis.datax.IGroupChildTaskIterator;
import com.qlangtech.tis.plugin.datax.SelectedTab;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-01-02 14:18
 **/
public class DataXKafkaGroupChildTaskIterator implements IGroupChildTaskIterator {
    private final DataXKafkaReader kafkaReader;
    // private final List<SelectedTab> tabs;
    private final Iterator<SelectedTab> tabIt;
    AtomicInteger taskIndex = new AtomicInteger(0);
    private final Map<String, List<DBDataXChildTask>> tabSet;

    public DataXKafkaGroupChildTaskIterator(DataXKafkaReader kafkaReader, List<SelectedTab> tabs) {
        this.kafkaReader = Objects.requireNonNull(kafkaReader, "kafkaReader can not be null");
        //   this.tabs = Objects.requireNonNull(tabs, "tabs can not be null");
        this.tabSet = tabs.stream().collect(Collectors.toMap((tab) -> tab.getName(), (tab) -> Lists.newArrayList()));
        this.tabIt = tabs.iterator();
    }

    public Map<String, List<DBDataXChildTask>> getGroupedInfo() {
        return this.tabSet;
    }


    @Override
    public void close() throws IOException {

    }

    @Override
    public boolean hasNext() {
        return tabIt.hasNext();
    }

    @Override
    public IDataxReaderContext next() {
        SelectedTab tab = tabIt.next();
        return new DataXKafkaReaderContext(tab.getName() + "_" + taskIndex.getAndIncrement(), tab);
    }
}
