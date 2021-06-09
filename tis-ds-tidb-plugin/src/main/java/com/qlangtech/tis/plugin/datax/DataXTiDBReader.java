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
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.IDataSourceDumper;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-05 09:00
 **/
public class DataXTiDBReader extends BasicDataXRdbmsReader {

    public static final String DATAX_NAME = "TiDB";

    @Override
    protected RdbmsReaderContext createDataXReaderContext(
            String jobName, SelectedTab tab, IDataSourceDumper dumper, DataSourceFactory dsFactory) {
        TiDBReaderContext readerContext = new TiDBReaderContext(jobName, tab.getName());
        return readerContext;
    }

    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataXTiDBReader.class, "tidb-reader-tpl.json");
    }

    @TISExtension()
    public static class DefaultDescriptor extends BaseDataxReaderDescriptor //implements FormFieldType.IMultiSelectValidator
    {
        public DefaultDescriptor() {
            super();
        }

        @Override
        public boolean isRdbms() {
            return true;
        }

//        @Override
//        public boolean validate(IFieldErrorHandler msgHandler, Context context, String fieldName, List<FormFieldType.SelectedItem> items) {
//            return true;
//        }

        @Override
        public String getDisplayName() {
            return DATAX_NAME;
        }
    }
}
