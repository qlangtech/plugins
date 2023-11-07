package com.qlangtech.tis.plugin.datax.powerjob.impl;

import com.qlangtech.tis.coredefine.module.action.RcHpaStatus;
import com.qlangtech.tis.coredefine.module.action.impl.RcDeployment;
import com.qlangtech.tis.datax.job.DataXJobWorker;
import com.qlangtech.tis.plugin.incr.WatchPodLog;
import com.qlangtech.tis.trigger.jst.ILogListener;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/11/3
 */
public class BasicPowerjobWorker extends DataXJobWorker  {

    @Override
    public void relaunch() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void relaunch(String podName) {
        throw new UnsupportedOperationException();
    }

    @Override
    public RcDeployment getRCDeployment() {
        throw new UnsupportedOperationException();
    }

    @Override
    public RcHpaStatus getHpaStatus() {
        throw new UnsupportedOperationException();
    }

    @Override
    public WatchPodLog listPodAndWatchLog(String podName, ILogListener listener) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void launchService(Runnable launchProcess) {
        throw new UnsupportedOperationException();
    }
}
