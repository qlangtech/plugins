/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.tis.plugin.datax;

import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.extension.PluginManager;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteJobTrigger;
import com.qlangtech.tis.fullbuild.indexbuild.RunningStatus;
import com.qlangtech.tis.order.center.IJoinTaskContext;
import com.tis.hadoop.rpc.RpcServiceReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-22 16:21
 **/
public class TaskExec {
    private static final Logger logger = LoggerFactory.getLogger(TaskExec.class);
    private static final ExecutorService dataXExecutor = newFixedThreadPool(10);// Executors.newCachedThreadPool();

    public static ExecutorService newFixedThreadPool(int nThreads) {
        return new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(40),
                Executors.defaultThreadFactory());
    }

    static IRemoteJobTrigger getiRemoteJobTrigger(IJoinTaskContext taskContext, RpcServiceReference statusRpc
            , IDataxProcessor dataxProcessor, String dataXfileName, PluginManager pluginManager) {
        final com.alibaba.datax.core.util.container.JarLoader uberClassLoader
                = new com.alibaba.datax.core.util.container.JarLoader(new String[]{"."}) {
            @Override
            protected Class<?> findClass(String name) throws ClassNotFoundException {

                try {
                    PluginManager.UberClassLoader classLoader = pluginManager.uberClassLoader;
                    return classLoader.findClass(name);
                } catch (Throwable e) {
                    throw new RuntimeException("className:" + name + ",scan the plugins:"
                            + pluginManager.activePlugins.stream().map((p) -> p.getDisplayName()).collect(Collectors.joining(",")), e);
                }
            }
        };

//        try {
//            System.out.println("aaaaaaaaaaaaaaaaaaaaaaa:" +
//                    uberClassLoader.loadClass("com.alibaba.datax.common.spi.Reader$Job"));
//        } catch (Exception e) {
//            System.out.println("********************aaaaaaaaaaaaaaaaaaaaaaa:" + e.getMessage());
//        }
//
//        try {
//            System.out.println("xxxxxxxxxxxxxxxxxxxxxx:"
//                    + this.getClass().getClassLoader().loadClass("com.alibaba.datax.common.spi.Reader"));
//            System.out.println("bbbbbbbbbbbbbbbbbbbbbbb:"
//                    + this.getClass().getClassLoader().loadClass("com.alibaba.datax.common.spi.Reader$Job"));
//        } catch (Exception e) {
//            System.out.println("********************bbbbbbbbbbbbbbbbbbbb:" + e.getMessage());
//        }

        com.qlangtech.tis.datax.DataxExecutor dataxExecutor = new com.qlangtech.tis.datax.DataxExecutor(statusRpc);

        File jobPath = new File(dataxProcessor.getDataxCfgDir(null), dataXfileName);
        AtomicBoolean complete = new AtomicBoolean(false);
        AtomicBoolean success = new AtomicBoolean(false);
        return new IRemoteJobTrigger() {
            @Override
            public void submitJob() {
                dataXExecutor.submit(() -> {
                    try {
                        dataxExecutor.startWork(taskContext.getIndexName()
                                , taskContext.getTaskId(), dataXfileName, jobPath.getAbsolutePath(), uberClassLoader);
                        success.set(true);
                    } catch (Throwable e) {
                        logger.error(jobPath.getAbsolutePath(), e);
                        success.set(false);
                        throw new RuntimeException(e);
                    } finally {
                        complete.set(true);
                    }
                });
            }

            @Override
            public RunningStatus getRunningStatus() {
                return new RunningStatus(0, complete.get(), success.get());
            }
        };
    }
}
