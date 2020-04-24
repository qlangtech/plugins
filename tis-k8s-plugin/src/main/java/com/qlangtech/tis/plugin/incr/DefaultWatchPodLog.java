/* * Copyright 2020 QingLang, Inc.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.qlangtech.tis.plugin.incr;

import com.google.common.collect.Lists;
import com.google.gson.reflect.TypeToken;
import com.squareup.okhttp.Call;
import com.qlangtech.tis.coredefine.module.action.LoopQueue;
import com.qlangtech.tis.trigger.jst.ILogListener;
import com.qlangtech.tis.trigger.socket.ExecuteState;
import com.qlangtech.tis.trigger.socket.InfoType;
import com.qlangtech.tis.trigger.socket.LogType;
import io.kubernetes.client.ApiClient;
import io.kubernetes.client.PodLogs;
import io.kubernetes.client.apis.CoreV1Api;
import io.kubernetes.client.models.V1ObjectMeta;
import io.kubernetes.client.models.V1Pod;
import io.kubernetes.client.models.V1PodStatus;
import io.kubernetes.client.util.Watch;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.InputStream;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/*
 * @create: 2020-04-12 16:02
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2020/04/13
 */
public class DefaultWatchPodLog extends WatchPodLog {

    private final List<ILogListener> listeners = Lists.newArrayList();

    private final LoopQueue<ExecuteState<String>> loopQueue = new LoopQueue<>(new ExecuteState[100]);

    private final Logger logger = LoggerFactory.getLogger(K8sIncrSync.class);

    private static final ExecutorService exec = Executors.newCachedThreadPool();

    private final ApiClient client;

    private final CoreV1Api api;

    private final String indexName;

    private final DefaultIncrK8sConfig config;

    public DefaultWatchPodLog(String indexName, ApiClient client, CoreV1Api api, final DefaultIncrK8sConfig config) {
        this.indexName = indexName;
        this.api = api;
        this.client = client;
        this.config = config;
    }

    @Override
    public void addListener(ILogListener listener) {
        synchronized (this) {
            ExecuteState<String>[] buffer = this.loopQueue.readBuffer();
            // 将缓冲区中的数据写入到外部监听者中
            for (int i = 0; i < buffer.length; i++) {
                if (buffer[i] == null || listener.isClosed()) {
                    break;
                }
                listener.read(buffer[i]);
            }
            this.listeners.add(listener);
        }
    }

    // private final ReentrantLock lock = new ReentrantLock();
    private final AtomicBoolean lock = new AtomicBoolean();

    @Override
    void startProcess() {
        if (lock.compareAndSet(false, true)) {
            logger.info("has gain the watch lock " + this.indexName);
            // this.api = new CoreV1Api(client);
            exec.execute(() -> {
                try {
                    Call call = api.listNamespacedPodCall(this.config.namespace, false, null, null, null, "app=" + indexName, 100, null, 600, true, null, null);
                    Watch<V1Pod> podWatch = Watch.createWatch(client, call, new TypeToken<Watch.Response<V1Pod>>() {
                    }.getType());
                    V1PodStatus status = null;
                    V1ObjectMeta metadata = null;
                    try {
                        for (Watch.Response<V1Pod> item : podWatch) {
                            status = item.object.getStatus();
                            if ("running".equalsIgnoreCase(status.getPhase())) {
                                metadata = item.object.getMetadata();
                                break;
                            }
                        }
                    } finally {
                        podWatch.close();
                    }
                    if (metadata != null) {
                        monitorPodLog(indexName, metadata.getNamespace(), metadata.getName());
                    }
                } catch (Exception e) {
                    logger.error("monitor " + this.indexName + " incr_log", e);
                    throw new RuntimeException(e);
                } finally {
                    lock.lazySet(false);
                }
            });
        } else {
            logger.info("has not gain the watch lock");
        }
    }

    private InputStream monitorLogStream;

    private void monitorPodLog(String indexName, String namespace, String podName) {
        try {
            PodLogs logs = new PodLogs(this.client);
            monitorLogStream = logs.streamNamespacedPodLog(namespace, podName, indexName);
            LineIterator lineIt = IOUtils.lineIterator(monitorLogStream, "utf8");
            while (lineIt.hasNext()) {
                ExecuteState<String> event = ExecuteState.create(InfoType.INFO, lineIt.nextLine());
                sendMsg(indexName, event);
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            try {
                IOUtils.closeQuietly(monitorLogStream);
            } catch (Throwable e) {
            }
        }
    }

    /**
     * 向监听者发送消息
     *
     * @param event
     */
    private void sendMsg(String indexName, ExecuteState<String> event) {
        event.setServiceName(indexName);
        event.setLogType(LogType.INCR_DEPLOY_STATUS_CHANGE);
        synchronized (this) {
            Iterator<ILogListener> lit = this.listeners.iterator();
            ILogListener l = null;
            while (lit.hasNext()) {
                l = lit.next();
                if (l.isClosed()) {
                    lit.remove();
                    continue;
                }
                loopQueue.write(event);
                l.read(event);
            }
        }
    }

    public void close() {
        try {
            IOUtils.closeQuietly(monitorLogStream);
        } catch (Throwable e) {
        }
        this.exec.shutdownNow();
    }
}
