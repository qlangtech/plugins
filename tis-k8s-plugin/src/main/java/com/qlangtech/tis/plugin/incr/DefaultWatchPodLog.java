/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 *   This program is free software: you can use, redistribute, and/or modify
 *   it under the terms of the GNU Affero General Public License, version 3
 *   or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 *  This program is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *   FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package com.qlangtech.tis.plugin.incr;

import com.google.common.collect.Sets;
import com.qlangtech.tis.coredefine.module.action.LoopQueue;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.trigger.jst.ILogListener;
import com.qlangtech.tis.trigger.socket.ExecuteState;
import com.qlangtech.tis.trigger.socket.LogType;
import io.kubernetes.client.PodLogs;
import io.kubernetes.client.openapi.ApiClient;
import io.kubernetes.client.openapi.apis.CoreV1Api;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InputStream;
import java.net.SocketTimeoutException;
import java.util.Iterator;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author 百岁（baisui@qlangtech.com）
 * @create: 2020-04-12 16:02
 * @date 2020/04/13
 */
public class DefaultWatchPodLog extends WatchPodLog {

    // 为了保证listeners不重复添加使用set
    private final Set<ILogListener> listeners = Sets.newHashSet();

    private final LoopQueue<ExecuteState> loopQueue = new LoopQueue<>(new ExecuteState[100]);

    private final Logger logger = LoggerFactory.getLogger(K8sIncrSync.class);

    private static final ExecutorService exec = Executors.newCachedThreadPool();

    private final ApiClient client;

    private final CoreV1Api api;

    private final String indexName;
    private final String podName;

    private final DefaultIncrK8sConfig config;

    public DefaultWatchPodLog(String indexName, String podName, ApiClient client, CoreV1Api api, final DefaultIncrK8sConfig config) {
        this.indexName = indexName;
        if (StringUtils.isBlank(podName)) {
            throw new IllegalArgumentException("param podName can not be null");
        }
        this.podName = podName;
        this.api = api;
        this.client = client;
        this.config = config;
    }

    @Override
    public void addListener(ILogListener listener) {
        try {
            synchronized (this) {
                ExecuteState[] buffer = this.loopQueue.readBuffer();
                // 将缓冲区中的数据写入到外部监听者中
                for (int i = 0; i < buffer.length; i++) {
                    if (buffer[i] == null || listener.isClosed()) {
                        break;
                    }
                    listener.sendMsg2Client(buffer[i]);
                }
                this.listeners.add(listener);
            }
            this.startProcess();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    // private final ReentrantLock lock = new ReentrantLock();
    private final AtomicBoolean lock = new AtomicBoolean();

    //private final ReentrantLock lock = new ReentrantLock();

    void startProcess() {
        if (this.lock.compareAndSet(false, true)) {
            // try {
            logger.info("has gain the watch lock " + this.indexName);
            // final CountDownLatch countdown = new CountDownLatch(1);
            exec.execute(() -> {
                try {
//                    Call call = api.listNamespacedPodCall(this.config.namespace, null, null, null, null, "app=" + indexName, 100, null, 600, true, null);
//                    Watch<V1Pod> podWatch = Watch.createWatch(client, call, new TypeToken<Watch.Response<V1Pod>>() {
//                    }.getType());
//                    V1PodStatus status = null;
                    //V1ObjectMeta metadata = null;
//                    try {
//                        for (Watch.Response<V1Pod> item : podWatch) {
//                            status = item.object.getStatus();
//                            if ("running".equalsIgnoreCase(status.getPhase())) {
//                                metadata = item.object.getMetadata();
//                                break;
//                            }
//                        }
//                    } finally {
//                        podWatch.close();
//                    }
                    //if (metadata != null) {
                    monitorPodLog(indexName, this.config.namespace, this.podName);
                    //}
                } catch (Throwable e) {
                    logger.error("monitor " + this.indexName + " incr_log", e);
                    throw new RuntimeException(e);
                } finally {
                    // countdown.countDown();
                    this.lock.set(false);
                }
            });
//            try {
//                countdown.await();
//            } catch (InterruptedException e) {
//                logger.error(e.getMessage(), e);
//            }
//            } finally {
//               // lock.unlock();
//            }
        } else {
            logger.info("has not gain the watch lock");
        }
    }

    private InputStream monitorLogStream;

    /**
     * @param indexName
     * @param namespace
     * @param podName
     * @return 是否是正常退出
     */
    private boolean monitorPodLog(String indexName, String namespace, String podName) {
        try {
            PodLogs logs = new PodLogs(this.client);
            // String namespace, String name, String container, Integer sinceSeconds, Integer tailLines, boolean timestamps
            // 显示200行
            monitorLogStream = logs.streamNamespacedPodLog(namespace, podName, indexName, null, 200, false);
            LineIterator lineIt = IOUtils.lineIterator(monitorLogStream, TisUTF8.get());
            ExecuteState event = null;
            boolean allConnectionDie = false;
            while (!allConnectionDie && lineIt.hasNext()) {
                event = ExecuteState.create(LogType.INCR_DEPLOY_STATUS_CHANGE, lineIt.nextLine());
                // 如果所有的监听者都死了，这里也就不用继续监听日志了
                allConnectionDie = sendMsg(indexName, event);
            }
        } catch (Throwable e) {
            if (ExceptionUtils.indexOfThrowable(e, SocketTimeoutException.class) > -1) {
                // 连接超时需要向客户端发一个信号告诉它连接失效了，以便再次重连
                logger.warn("indexName:" + indexName + " monitor pod:" + podName + " logs timeout");
                try {
                    sendMsg(indexName, ExecuteState.create(LogType.INCR_DEPLOY_STATUS_CHANGE, new ExecuteState.TimeoutResult()));
                } catch (Throwable ex) {
                }
                return false;
            }
            throw new RuntimeException("indexName:" + indexName + ",namespace:" + namespace + ",podName:" + podName, e);
        } finally {
            this.clearStatConnection();
        }
        return true;
    }

    /**
     * 向监听者发送消息
     *
     * @param event
     * @return 是否所有的监听者都死了？
     */
    private boolean sendMsg(String indexName, ExecuteState event) throws IOException {
        event.setServiceName(indexName);
        // event.setLogType(LogType.INCR_DEPLOY_STATUS_CHANGE);
        boolean allConnectionDie = true;
        synchronized (this) {
            Iterator<ILogListener> lit = this.listeners.iterator();
            ILogListener l = null;
            while (lit.hasNext()) {
                l = lit.next();
                if (l.isClosed()) {
                    lit.remove();
                    continue;
                } else {
                    allConnectionDie = false;
                }
                loopQueue.write(event);
                l.sendMsg2Client(event);
            }
        }
        return allConnectionDie;
    }

    @Override
    public void close() {
        clearStatConnection();
        //this.exec.shutdownNow();
    }

    /**
     * 清除有状态的连接
     */
    private void clearStatConnection() {
        try {
            IOUtils.closeQuietly(monitorLogStream);
        } catch (Throwable e) {
        }
        loopQueue.cleanBuffer();
        this.listeners.clear();
    }
}
