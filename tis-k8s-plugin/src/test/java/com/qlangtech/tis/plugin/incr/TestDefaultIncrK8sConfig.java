package com.qlangtech.tis.plugin.incr;

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.coredefine.module.action.IIncrSync;
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
        IIncrSync incrSync = getIncrSync();
        String podName = "podname";

        assertNotNull(incrSync);
        final AtomicInteger msgReceiveCount = new AtomicInteger();
        incrSync.listPodAndWatchLog(totalpay, podName, new ILogListener() {
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

    public static IIncrSync getIncrSync() {
        PluginStore<IncrStreamFactory> store = TIS.getPluginStore(totalpay, IncrStreamFactory.class);
        assertNotNull(store);
        IncrStreamFactory incrStream = store.getPlugin();
        assertNotNull(incrStream);

        IIncrSync incrSync = incrStream.getIncrSync();
        assertNotNull(incrSync);
        return incrSync;
    }
}
