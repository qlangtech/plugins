/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qlangtech.tis.plugin.datax;

import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.datax.*;
import com.qlangtech.tis.datax.impl.DataXCfgGenerator;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.manage.IAppSource;
import com.qlangtech.tis.plugin.KeyedPluginStore;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.sql.parser.SqlTaskNodeMeta;
import com.qlangtech.tis.sql.parser.TopologyDir;
import com.qlangtech.tis.util.IPluginContext;

import java.io.File;
import java.io.IOException;
import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-01-05 22:10
 **/
public class DataFlowDataXProcessor implements IDataxProcessor, IAppSource {
    @FormField(ordinal = 1, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String globalCfg;

    private final String dataFlowName;

    public DataFlowDataXProcessor(String dataFlowName) {
        this.dataFlowName = dataFlowName;
    }

    @Override
    public IDataxReader getReader(IPluginContext pluginCtx) {
        return DataxReader.load(pluginCtx, true, this.dataFlowName);
    }

    @Override
    public IDataxGlobalCfg getDataXGlobalCfg() {
        IDataxGlobalCfg globalCfg = ParamsConfig.getItem(this.globalCfg, IDataxGlobalCfg.KEY_DISPLAY_NAME);
        Objects.requireNonNull(globalCfg, "dataX Global config can not be null");
        return globalCfg;
    }


    @Override
    public IDataxWriter getWriter(IPluginContext pluginCtx) {
        return DataxWriter.load(pluginCtx, KeyedPluginStore.StoreResourceType.DataFlow, this.dataFlowName);
    }

    @Override
    public File getDataXWorkDir(IPluginContext pluginContext) {
        TopologyDir topoDir = SqlTaskNodeMeta.getTopologyDir(this.dataFlowName);
        return topoDir.getLocalSubFileDir();
    }

    @Override
    public File getDataxCfgDir(IPluginContext pluginCtx) {
        return DataxProcessor.getDataxCfgDir(pluginCtx, this);
    }

    @Override
    public File getDataxCreateDDLDir(IPluginContext pluginContext) {
        return DataxProcessor.getDataxCreateDDLDir(pluginContext, this);
    }

    @Override
    public void saveCreateTableDDL(IPluginContext pluginCtx, StringBuffer createDDL, String sqlFileName, boolean overWrite) throws IOException {

    }


    @Override
    public boolean isReaderUnStructed(IPluginContext pluginCtx) {
        return false;
    }

    @Override
    public boolean isRDBMS2UnStructed(IPluginContext pluginCtx) {
        return false;
    }

    @Override
    public boolean isRDBMS2RDBMS(IPluginContext pluginCtx) {
        return false;
    }

    @Override
    public boolean isWriterSupportMultiTableInReader(IPluginContext pluginCtx) {
        return false;
    }

    @Override
    public DataXCfgGenerator.GenerateCfgs getDataxCfgFileNames(IPluginContext pluginCtx) {
        return DataxProcessor.getDataxCfgFileNames(pluginCtx, this);
    }

    @Override
    public TableAliasMapper getTabAlias() {
        return null;
    }
}
