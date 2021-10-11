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

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsWriter;
import com.qlangtech.tis.plugin.ds.oracle.OracleDataSourceFactory;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;

import java.util.Optional;

/**
 * @author: baisui 百岁
 * @create: 2021-04-07 15:30
 * @see com.alibaba.datax.plugin.writer.oraclewriter.TISOracleWriter
 **/
public class DataXOracleWriter extends BasicDataXRdbmsWriter<OracleDataSourceFactory> {


    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataXOracleWriter.class, "DataXOracleWriter-tpl.json");
    }

    @Override
    public IDataxContext getSubTask(Optional<IDataxProcessor.TableMap> tableMap) {
        if (!tableMap.isPresent()) {
            throw new IllegalStateException("tableMap must be present");
        }
        OracleWriterContext writerContext = new OracleWriterContext(this, tableMap.get());
        return writerContext;
    }

    @Override
    public StringBuffer generateCreateDDL(IDataxProcessor.TableMap tableMapper) {
        if (!this.autoCreateTable) {
            return null;
        }
        StringBuffer createDDL = new StringBuffer();
        return createDDL;
    }

    @TISExtension()
    public static class DefaultDescriptor extends RdbmsWriterDescriptor {
        @Override
        public String getDisplayName() {
            return OracleDataSourceFactory.ORACLE;
        }

        public boolean validateSession(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            return DataXOracleReader.validateSession(msgHandler, context, fieldName, value);
        }
    }
}
