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

package com.qlangtech.tis.realtime.source;

import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-07-06 16:46
 **/
public class HttpSourceEnumerator implements SplitEnumerator<HttpSourceSplit, Collection<HttpSourceSplit>> {

    private final ArrayBlockingQueue<HttpSourceSplit> splits;
    private final SplitEnumeratorContext<HttpSourceSplit> context;

    public HttpSourceEnumerator(List<HttpSourceSplit> splits, SplitEnumeratorContext<HttpSourceSplit> context) {
        this.splits = new ArrayBlockingQueue<>(splits.size(), true, splits);
        this.context = context;
    }

    @Override
    public void start() {
        // 初始分配所有分片
        //assignSplits();
        Set<Integer> readerIDs = context.registeredReaders().keySet();
        for (int subtaskId : readerIDs) {
            assignNextSplit(subtaskId);
        }
    }

    private void assignNextSplit(int subtaskId) {
        HttpSourceSplit existSplit = splits.poll();
        // assignNextSplitToReader(subtaskId);
        if (existSplit != null) {
            context.assignSplit(existSplit, subtaskId);
        } else {
            context.signalNoMoreSplits(subtaskId);
        }
    }


    @Override
    public void handleSplitRequest(int subtaskId, String requesterHostname) {
        // 按需分配分片
        assignNextSplit(subtaskId);
    }

    @Override
    public void addSplitsBack(List<HttpSourceSplit> splits, int subtaskId) {
        this.splits.addAll(splits);
    }

    @Override
    public void addReader(int subtaskId) {
        // 新 reader 注册时分配分片
        assignNextSplit(subtaskId);
    }

    @Override
    public Collection<HttpSourceSplit> snapshotState(long checkpointId) throws Exception {
        return splits;
    }


    @Override
    public void close() throws IOException {
        // 清理资源
    }
}
