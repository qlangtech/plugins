package com.qlangtech.tis.plugin.datax.powerjob.impl.coresource;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.datax.powerjob.PowerjobCoreDataSource;

/**
 * 使用Powerjob 默认的
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/10/31
 */
public class EmbeddedPowerjobCoreDataSource extends PowerjobCoreDataSource {


    @TISExtension
    public static class DefaultDesc extends Descriptor<PowerjobCoreDataSource> {
        @Override
        public String getDisplayName() {
            return "Embedded";
        }
    }
}
