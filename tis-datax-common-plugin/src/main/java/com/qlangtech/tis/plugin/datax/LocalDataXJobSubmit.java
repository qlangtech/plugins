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

import com.alibaba.datax.core.util.container.JarLoader;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.datax.DataXJobSubmit;
import com.qlangtech.tis.datax.DataxExecutor;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.extension.PluginManager;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteJobTrigger;
import com.qlangtech.tis.fullbuild.indexbuild.RunningStatus;
import com.qlangtech.tis.order.center.IJoinTaskContext;
import com.tis.hadoop.rpc.RpcServiceReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Objects;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-04-27 17:28
 **/
@TISExtension()
public class LocalDataXJobSubmit extends DataXJobSubmit {

    private static final Logger logger = LoggerFactory.getLogger(LocalDataXJobSubmit.class);
    private static final ExecutorService dataXExecutor = newFixedThreadPool(10);// Executors.newCachedThreadPool();

    public static ExecutorService newFixedThreadPool(int nThreads) {
        return new ThreadPoolExecutor(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<Runnable>(40),
                Executors.defaultThreadFactory());
    }

    @Override
    public InstanceType getType() {
        return InstanceType.LOCAL;
    }

    @Override
    public IRemoteJobTrigger createDataXJob(IJoinTaskContext taskContext, RpcServiceReference statusRpc
            , IDataxProcessor dataxProcessor, String dataXfileName) {
        Objects.requireNonNull(statusRpc, "statusRpc can not be null");

        final JarLoader uberClassLoader = new JarLoader(new String[]{"."}, this.getClass().getClassLoader()) {
            @Override
            protected Class<?> findClass(String name) throws ClassNotFoundException {
                PluginManager pluginManager = TIS.get().getPluginManager();
                try {
                    PluginManager.UberClassLoader classLoader = pluginManager.uberClassLoader;
                    return classLoader.findClass(name);
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException("scan the plugins:"
                            + pluginManager.activePlugins.stream().map((p) -> p.getDisplayName()).collect(Collectors.joining(",")), e);
                }
            }
        };

        try {
            System.out.println("aaaaaaaaaaaaaaaaaaaaaaa:" +
                    uberClassLoader.loadClass("com.alibaba.datax.common.spi.Reader"));
        } catch (Exception e) {
            System.out.println("********************aaaaaaaaaaaaaaaaaaaaaaa");
        }

        try {
            System.out.println("bbbbbbbbbbbbbbbbbbbbbbb:"
                    + this.getClass().getClassLoader().loadClass("com.alibaba.datax.common.spi.Reader"));
        } catch (ClassNotFoundException e) {
            System.out.println("********************bbbbbbbbbbbbbbbbbbbb");
        }

        DataxExecutor dataxExecutor = new DataxExecutor(statusRpc, uberClassLoader);

        File jobPath = new File(dataxProcessor.getDataxCfgDir(), dataXfileName);
        AtomicBoolean complete = new AtomicBoolean(false);
        AtomicBoolean success = new AtomicBoolean(false);
        return new IRemoteJobTrigger() {
            @Override
            public void submitJob() {
                dataXExecutor.submit(() -> {
                    try {
                        dataxExecutor.startWork(taskContext.getIndexName()
                                , taskContext.getTaskId(), dataXfileName, jobPath.getAbsolutePath());
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
