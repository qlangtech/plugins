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
package com.qlangtech.tis.plugin.ontology.sync;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * 本体 Neo4j 同步异步队列。由单个守护线程消费，不阻塞调用方（afterSaved 回调）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/5/28
 */
public class OntologySyncQueue {

    private static final Logger log = LoggerFactory.getLogger(OntologySyncQueue.class);
    private static final LinkedBlockingQueue<Runnable> QUEUE = new LinkedBlockingQueue<>();

    static {
        Thread worker = new Thread(() -> {
            while (!Thread.currentThread().isInterrupted()) {
                try {
                    Runnable task = QUEUE.take();
                    task.run();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                } catch (Exception e) {
                    log.warn("[OntologySyncQueue] sync task error", e);
                }
            }
        }, "ontology-neo4j-sync");
        worker.setDaemon(true);
        worker.start();
    }

    /** 提交一个同步任务到队列（非阻塞）。 */
    public static void enqueue(Runnable task) {
        QUEUE.offer(task);
    }
}