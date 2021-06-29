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

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsReader;
import com.qlangtech.tis.plugin.datax.common.RdbmsReaderContext;
import com.qlangtech.tis.plugin.ds.IDataSourceDumper;
import com.qlangtech.tis.plugin.ds.tidb.TiKVDataSourceFactory;

/**
 * @see com.qlangtech.tis.plugin.datax.TiKVReader
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-05 09:00
 **/
public class DataXTiDBReader extends BasicDataXRdbmsReader<TiKVDataSourceFactory> {

    public static final String DATAX_NAME = "TiDB";

    @Override
    protected RdbmsReaderContext createDataXReaderContext(
            String jobName, SelectedTab tab, IDataSourceDumper dumper) {
        TiDBReaderContext readerContext = new TiDBReaderContext(jobName, tab.getName(), dumper, this);
        return readerContext;
    }




    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataXTiDBReader.class, "tidb-reader-tpl.json");
    }

    @TISExtension()
    public static class DefaultDescriptor extends BasicDataXRdbmsReaderDescriptor //implements FormFieldType.IMultiSelectValidator
    {
        @Override
        public String getDisplayName() {
            return DATAX_NAME;
        }
    }
}
