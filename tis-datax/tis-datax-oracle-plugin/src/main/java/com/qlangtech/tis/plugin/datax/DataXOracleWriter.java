/**
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.qlangtech.tis.plugin.datax;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.annotation.Public;
import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsWriter;
import com.qlangtech.tis.plugin.datax.common.InitWriterTable;
import com.qlangtech.tis.plugin.ds.oracle.OracleDataSourceFactory;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;

import java.util.List;
import java.util.Optional;

/**
 * @author: baisui 百岁
 * @create: 2021-04-07 15:30
 * @see com.alibaba.datax.plugin.writer.oraclewriter.TISOracleWriter
 **/
@Public
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
    public void initWriterTable(String targetTabName, List<String> jdbcUrls) throws Exception {
        InitWriterTable.process(this.dataXName, targetTabName, jdbcUrls);
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
