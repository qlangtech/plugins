package com.qlangtech.tis.plugin.datax;

import com.qlangtech.tis.fullbuild.indexbuild.RemoteTaskTriggers;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/11/9
 */
public class PowerJobTskTriggers extends RemoteTaskTriggers {
    public PowerJobTskTriggers() {
        super(null);
    }

    @Override
    public void allCancel() {
        throw new UnsupportedOperationException();
    }
}
