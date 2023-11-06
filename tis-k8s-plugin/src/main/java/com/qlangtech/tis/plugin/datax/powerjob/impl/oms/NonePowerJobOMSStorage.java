package com.qlangtech.tis.plugin.datax.powerjob.impl.oms;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.datax.powerjob.PowerJobOMSStorage;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/10/31
 */
public class NonePowerJobOMSStorage extends PowerJobOMSStorage {

    @TISExtension
    public static class DefaultDesc extends Descriptor<PowerJobOMSStorage> {
        @Override
        public String getDisplayName() {
            return "None";
        }
    }
}
