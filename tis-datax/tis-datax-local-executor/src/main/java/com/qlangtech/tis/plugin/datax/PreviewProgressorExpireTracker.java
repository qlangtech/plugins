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

package com.qlangtech.tis.plugin.datax;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.invoke.MethodHandles;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-07-26 12:06
 **/
public class PreviewProgressorExpireTracker implements Runnable {
    private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

    // settings, not final so we can change them in testing
    //  private int docsUpperBound;
    private final long timeUpperBound;
    //  private long tLogFileSizeUpperBound;

    private long lastProcessTime;

    // note: can't use ExecutorsUtil because it doesn't have a *scheduled* ExecutorService.
    //  Not a big deal but it means we must take care of MDC logging here.
    private final ScheduledExecutorService scheduler =
            Executors.newScheduledThreadPool(1, new PreviewProgressorExpireTrackerThreadFactory("commitScheduler"));
    private ScheduledFuture<?> pending;


    private String name;

    public PreviewProgressorExpireTracker(
            String name,
            long timeUpperBound) {
        this.name = name;
        pending = null;
        this.timeUpperBound = timeUpperBound;
    }


    public synchronized void close() {
        if (pending != null) {
            pending.cancel(false);
            pending = null;
        }
        scheduler.shutdown();
    }

    /**
     * 每次预览数据需要调用此方法，顺便将本地进程续命
     */
    public void scheduleCommitWithin(long commitMaxTime) {
        this.lastProcessTime = System.currentTimeMillis();
        _scheduleCommitWithinIfNeeded(commitMaxTime);
    }

    public void cancelPendingCommit() {
        synchronized (this) {
            if (pending != null) {
                boolean canceled = pending.cancel(false);
                if (canceled) {
                    pending = null;
                }
            }
        }
    }

    private void _scheduleCommitWithinIfNeeded(long commitWithin) {
        long ctime = (commitWithin > 0) ? commitWithin : timeUpperBound;

        if (ctime > 0) {
            _scheduleCommitWithin(ctime);
        }
    }

    private void _scheduleCommitWithin(long commitMaxTime) {
        if (commitMaxTime <= 0) return;
        synchronized (this) {

            if (pending != null && pending.getDelay(TimeUnit.MILLISECONDS) <= commitMaxTime) {
                // There is already a pending commit that will happen first, so
                // nothing else to do here.
                // log.info("###returning since getDelay()=={} less than {}",
                // pending.getDelay(TimeUnit.MILLISECONDS), commitMaxTime);

                return;
            }

            if (pending != null) {
                // we need to schedule a commit to happen sooner than the existing one,
                // so lets try to cancel the existing one first.
                boolean canceled = pending.cancel(false);
                if (!canceled) {
                    // It looks like we can't cancel... it must have just started running!
                    // this is possible due to thread scheduling delays and a low commitMaxTime.
                    // Nothing else to do since we obviously can't schedule our commit *before*
                    // the one that just started running (or has just completed).
                    // log.info("###returning since cancel failed");
                    return;
                }
            }

            // log.info("###scheduling for " + commitMaxTime);

            // schedule our new commit
            pending = scheduler.schedule(this, commitMaxTime, TimeUnit.MILLISECONDS);
        }
    }


    /**
     * This is the worker part for the ScheduledFuture *
     */
    @Override
    public void run() {
        synchronized (this) {
            log.info("###start expire process. pending=null");
            pending = null; // allow a new commit to be scheduled

            // 执行预定动作
            long current = System.currentTimeMillis();
            // 没有操作的时间
            long inactiveGap = (current - this.lastProcessTime);
            if (inactiveGap >= this.timeUpperBound) {
                log.info("inactiveGap:{} >= timeUpperBound:{},start executeExpirEvent", inactiveGap, timeUpperBound);
                // 执行失效操作，需要将之前的缓存踢除
                this.executeExpirEvent();
                this.close();
            } else {
                log.info("inactiveGap:{} < timeUpperBound:{},new inactiveGrp:{} ,start _scheduleCommitWithinIfNeeded"
                        , inactiveGap, timeUpperBound, this.timeUpperBound);
                // 不满足失效操作，需要继续等待
                this._scheduleCommitWithinIfNeeded(this.timeUpperBound);
            }
        }
    }

    protected void executeExpirEvent() {

    }


    // only for testing - not thread safe
    public boolean hasPending() {
        return (null != pending && !pending.isDone());
    }
}
