package com.qlangtech.tis.plugin.datax.dameng.reader;

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsReader;
import com.qlangtech.tis.plugin.datax.common.RdbmsReaderContext;
import com.qlangtech.tis.plugin.datax.dameng.ds.DaMengDataSourceFactory;
import com.qlangtech.tis.plugin.ds.IDataSourceDumper;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/9/14
 */
public class DataXDaMengReader extends BasicDataXRdbmsReader<DaMengDataSourceFactory> {
    public static final String DATAX_NAME = "DaMeng";

    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataXDaMengReader.class, "dameng-reader-tpl.vm");
    }

    @Override
    protected RdbmsReaderContext createDataXReaderContext(String jobName, SelectedTab tab, IDataSourceDumper dumper) {

        return new DaMengReaderContext(jobName, tab.getName(), dumper, this);
    }

    @TISExtension()
    public static class DefaultDescriptor extends BasicDataXRdbmsReaderDescriptor {
        public DefaultDescriptor() {
            super();
        }

        @Override
        public boolean isSupportIncr() {
            return false;
        }

        @Override
        public String getDisplayName() {
            return DATAX_NAME;
        }

        @Override
        public EndType getEndType() {
            return EndType.DaMeng;
        }
    }
}
