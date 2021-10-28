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

package com.qlangtech.tis.plugin.incr;

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.coredefine.module.action.IRCController;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.plugin.PluginStore;
import com.qlangtech.tis.trigger.jst.ILogListener;
import junit.framework.TestCase;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author: baisui 百岁
 * @create: 2020-08-17 11:22
 **/
public class TestDefaultIncrK8sConfig extends TestCase {
    public static final String totalpay = "search4totalpay";
//    public void testDefaultIncrK8sConfig() throws Exception {
//        DefaultIncrK8sConfig incrK8sCfg = new DefaultIncrK8sConfig();
//        incrK8sCfg.namespace = "tis";
//        incrK8sCfg.k8sName = "minikube";
//        incrK8sCfg.imagePath = "registry.cn-hangzhou.aliyuncs.com/tis/tis-incr:latest";
//
//        incrK8sCfg.getIncrSync().removeInstance("search4totalpay");
//    }

    public void testlistPodsAndWatchLog() throws Exception {
        IRCController incrSync = getIncrSync();
        String podName = "podname";

        assertNotNull(incrSync);
        final AtomicInteger msgReceiveCount = new AtomicInteger();
        incrSync.listPodAndWatchLog(new TargetResName(totalpay), podName, new ILogListener() {
            @Override
            public void sendMsg2Client(Object biz) throws IOException {

                System.out.println("flushCount:" + msgReceiveCount.incrementAndGet());
            }

            @Override
            public void read(Object event) {

            }

            @Override
            public boolean isClosed() {
                return false;
            }
        });
        Thread.sleep(5000);
        assertTrue(msgReceiveCount.get() > 0);
    }

    public static IRCController getIncrSync() {
        PluginStore<IncrStreamFactory> store = TIS.getPluginStore(totalpay, IncrStreamFactory.class);
        assertNotNull(store);
        IncrStreamFactory incrStream = store.getPlugin();
        assertNotNull(incrStream);

        IRCController incrSync = incrStream.getIncrSync();
        assertNotNull(incrSync);
        return incrSync;
    }
}
