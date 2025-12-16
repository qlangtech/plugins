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

import com.alibaba.citrus.turbine.Context;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.datax.AdapterDataxReader;
import com.qlangtech.tis.datax.DBDataXChildTask;
import com.qlangtech.tis.datax.DataXName;
import com.qlangtech.tis.datax.IDataxGlobalCfg;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.datax.IDataxWriter;
import com.qlangtech.tis.datax.IGroupChildTaskIterator;
import com.qlangtech.tis.datax.StoreResourceType;
import com.qlangtech.tis.datax.StoreResourceTypeConstants;
import com.qlangtech.tis.datax.TableAliasMapper;
import com.qlangtech.tis.datax.impl.DataXCfgGenerator;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.datax.impl.TransformerInfo;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.SuFormProperties;
import com.qlangtech.tis.extension.impl.XmlFile;
import com.qlangtech.tis.manage.IAppSource;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.IPluginStore.AfterPluginSaved;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.KeyedPluginStore;
import com.qlangtech.tis.plugin.KeyedPluginStore.Key;
import com.qlangtech.tis.plugin.PluginStore;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.AdapterSelectedTab;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.IDataSourceFactoryGetter;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.trigger.JobTrigger;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.sql.parser.SqlTaskNodeMeta;
import com.qlangtech.tis.sql.parser.TopologyDir;
import com.qlangtech.tis.sql.parser.meta.DependencyNode;
import com.qlangtech.tis.util.HeteroEnum;
import com.qlangtech.tis.util.IPluginContext;
import com.qlangtech.tis.util.TransformerRuleKey;
import com.qlangtech.tis.util.UploadPluginMeta;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FalseFileFilter;
import org.apache.commons.io.filefilter.SuffixFileFilter;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.File;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-01-05 22:10
 * @see DefaultDataxProcessor
 **/
public class DataFlowDataXProcessor implements IDataxProcessor, IAppSource, IdentityName, AfterPluginSaved {


    @FormField(ordinal = 1, identity = true, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
    public String name;
    @FormField(ordinal = 2, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String globalCfg;

    private transient SqlTaskNodeMeta.SqlDataFlowTopology _dataFlowTopology;

    @Override
    public String identityValue() {
        return this.name;
    }

    @Override
    public final StoreResourceType getResType() {
        return StoreResourceType.DataFlow;
    }

    @Override
    public IDataxReader getReader(IPluginContext pluginCtx) {
        // return DataxReader.load(pluginCtx, true, this.name);
        throw new UnsupportedOperationException("dataflow processor not support single reader getter");
    }


    @Override
    public void afterSaved(IPluginContext pluginContext, Optional<Context> context) {
        this.refresh();
    }

    @Override
    public void refresh() {
        _dataFlowTopology = null;
    }

    @Override
    public Set<TransformerInfo> getTransformerInfo(IPluginContext pluginCtx, Map<String, List<DBDataXChildTask>> groupedChildTask) {
        Set<TransformerInfo> tinfos = new HashSet<>();
        Map<String, SelectedTabs> dbIds = getDB2TabsMapper();
        for (Map.Entry<String, SelectedTabs> entry : dbIds.entrySet()) {
            addTransformerInfo(tinfos, pluginCtx, groupedChildTask
                    , StoreResourceType.DataBase, entry.getKey(), (tabName, context) -> {
                        Pair<List<RecordTransformerRules>, IPluginStore> tabTransformerRule
                                = loadRecordTransformerRulesAndPluginStore(context, StoreResourceType.DataBase, entry.getKey(), tabName);
                        for (RecordTransformerRules trule : tabTransformerRule.getKey()) {
                            return Optional.of(trule);
                        }
                        return Optional.empty();
                    });
        }
        return tinfos;
    }


    /**
     * @param tinfos
     * @param pluginCtx
     * @param groupedChildTask key: tableName
     * @param resType
     * @param pipeName
     */
    static void addTransformerInfo(Set<TransformerInfo> tinfos,
                                   IPluginContext pluginCtx
            , Map<String, List<DBDataXChildTask>> groupedChildTask, StoreResourceType resType, String pipeName
            , BiFunction<String, IPluginContext, Optional<RecordTransformerRules>> loadTransformerRules) {
        if (resType == StoreResourceType.DataBase) {
            // 由于在DataBase的情况下 使用 execId=99a716f6-c5f9-6cdd-34e9-9ab5835c29a9#writer 无效，所以重新创建一个pluginCtx实例
            pluginCtx = IPluginContext.namedContext(new DataXName(pipeName, resType));
        }
        Key transformerRuleKey = TransformerRuleKey.createStoreKey(pluginCtx, resType, pipeName, "dump");
        XmlFile sotre = transformerRuleKey.getSotreFile();
        File parent = sotre.getFile().getParentFile();
        if (!parent.exists()) {
            // return Collections.emptySet();
            return;
        }
        Optional<RecordTransformerRules> transformerRules = null;
        String xmlExtend = Descriptor.getPluginFileName(org.apache.commons.lang.StringUtils.EMPTY);
        SuffixFileFilter filter = new SuffixFileFilter(xmlExtend);
        Collection<File> matched = FileUtils.listFiles(parent, filter, FalseFileFilter.INSTANCE);
        for (File tfile : matched) {
            String tabName = org.apache.commons.lang.StringUtils.substringBefore(tfile.getName(), xmlExtend);
            if (StringUtils.isEmpty(tabName)) {
                throw new IllegalStateException("tabName can not be null");
            }
            if (groupedChildTask.containsKey(tabName)) {
                transformerRules = loadTransformerRules.apply(tabName, pluginCtx);
//                RecordTransformerRules.loadTransformerRules(
//                        pluginCtx, resType, pipeName, tabName);
                if (transformerRules.isPresent()) {
                    tinfos.add(new TransformerInfo(resType, pipeName, tabName, transformerRules.get().rules.size()));
                }
            }
        }
    }

    private String getDBNameByTable(String tabName) {
        Map<String, SelectedTabs> dbIds = getDB2TabsMapper();
        for (Map.Entry<String, SelectedTabs> entry : dbIds.entrySet()) {
            if (entry.getValue().contains(tabName)) {
                return entry.getKey();
            }
        }

        throw new IllegalStateException("table:" + tabName + " can not find matched dbId,in:"
                + dbIds.entrySet().stream().map((e) -> e.getKey()
                + "[" + String.valueOf(e.getValue()) + "]").collect(Collectors.joining(",")));
    }

    @Override
    public IDataxReader getReader(IPluginContext pluginContext, ISelectedTab tab) {
        return DataxReader.load(null, true, getDBNameByTable(Objects.requireNonNull(tab).getName()));
    }

    @Override
    public Pair<List<RecordTransformerRules>, IPluginStore> getRecordTransformerRulesAndPluginStore(IPluginContext pluginCtx, String tableName) {
        if (org.apache.commons.lang3.StringUtils.isEmpty(tableName)) {
            throw new IllegalArgumentException("param tableName:" + tableName + " can not be empty");
        }

        String appname = this.getDBNameByTable(tableName);
        StoreResourceType resourceType = StoreResourceType.DataBase;

        return loadRecordTransformerRulesAndPluginStore(pluginCtx, resourceType, appname, tableName);
    }


    public static Pair<List<RecordTransformerRules>, IPluginStore>
    loadRecordTransformerRulesAndPluginStore(IPluginContext pluginCtx, StoreResourceType resourceType, String appname, String tableName) {
        try {
            String rawContent = HeteroEnum.TRANSFORMER_RULES.identity + ":require,"
                    + SuFormProperties.SuFormGetterContext.FIELD_SUBFORM_ID + "_" + tableName
                    + "," + StoreResourceType.KEY_PROCESS_MODEL + "_" + resourceType.getType()
                    + "," + StoreResourceType.getPipeParma(resourceType, appname);
            return HeteroEnum.TRANSFORMER_RULES.getPluginsAndStore(pluginCtx, UploadPluginMeta.parse(rawContent));
        } catch (Throwable e) {
            throw new RuntimeException("tableName:" + tableName, e);
        }
    }

    @Override
    public List<IDataxReader> getReaders(IPluginContext pluginCtx) {
        try {
            Map<String, SelectedTabs> dbIds = getDB2TabsMapper();
            List<IDataxReader> readers = Lists.newArrayList();
            dbIds.entrySet().forEach((entry) -> {
                readers.add(new AdapterDataxReader(DataxReader.load(null, true, entry.getKey())) {
                    @Override
                    public IGroupChildTaskIterator getSubTasks() {
                        return super.getSubTasks((tab) -> entry.getValue().contains(tab));
                    }

                    @Override
                    public List<TopologySelectedTab> getSelectedTabs() {
                        return super.getSelectedTabs().stream()//
                                .filter((tab) -> entry.getValue().contains(tab))
                                .map((tab) -> new TopologySelectedTab(tab, entry.getValue().getTopologyId(tab))) //
                                .collect(Collectors.toList());
                    }
                });
            });

            return readers;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private Map<String, SelectedTabs> getDB2TabsMapper() {
        SqlTaskNodeMeta.SqlDataFlowTopology topology = getTopology();

        List<DependencyNode> dumpNodes = topology.getDumpNodes();

        Map<String/*dbName*/, SelectedTabs> dbIds = Maps.newHashMap();
        SelectedTabs tabs = null;
        for (DependencyNode dump : dumpNodes) {
            tabs = dbIds.get(dump.getDbName());
            if (tabs == null) {
                tabs = new SelectedTabs();
                dbIds.put(dump.getDbName(), tabs);
            }
            tabs.addDumpNode(dump);
        }
        return dbIds;
    }

    @Override
    public void setPluginStore(PluginStore<IAppSource> pluginStore) {

    }

    public static class TopologySelectedTab extends AdapterSelectedTab {
        private final String topologyId;

        public TopologySelectedTab(ISelectedTab target, String topologyId) {
            super(target);
            this.topologyId = topologyId;
        }

        public String getTopologyId() {
            return this.topologyId;
        }
    }

    private static class SelectedTabs {
        private final Map<String /*tabName*/, String/*toplogId*/> tab2ToplogId = Maps.newHashMap();

        public void addDumpNode(DependencyNode dumpNode) {
            tab2ToplogId.put(dumpNode.getName(), dumpNode.getId());
        }

        public boolean contains(ISelectedTab tab) {
            return this.contains(tab.getName());
        }

        public boolean contains(String tableName) {
            return tab2ToplogId.containsKey(tableName);
        }

        public String getTopologyId(ISelectedTab tab) {
            return Objects.requireNonNull(tab2ToplogId.get(tab.getName()) //
                    , "tabName:" + tab.getName() + " relevant topologyName can not be null");
        }

        @Override
        public String toString() {
            return String.join(",", this.tab2ToplogId.keySet());
        }
    }

    public SqlTaskNodeMeta.SqlDataFlowTopology getTopology() {
        if (_dataFlowTopology == null) {
            try {
                _dataFlowTopology = SqlTaskNodeMeta.getSqlDataFlowTopology(this.name);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        return _dataFlowTopology;
    }

    @Override
    public IDataxGlobalCfg getDataXGlobalCfg() {
        IDataxGlobalCfg globalCfg = ParamsConfig.getItem(this.globalCfg, IDataxGlobalCfg.KEY_DISPLAY_NAME);
        Objects.requireNonNull(globalCfg, "dataX Global config can not be null");
        return globalCfg;
    }


    @Override
    public IDataxWriter getWriter(IPluginContext pluginCtx, boolean validateNull) {
        return DataxWriter.load(pluginCtx, StoreResourceType.DataFlow, this.name, validateNull);
    }


    public DataSourceFactory getWriterDataSourceFactory() {
        DataSourceFactory writerDS = null;

        IDataxWriter writer = this.getWriter(null);
        if (!(writer instanceof IDataSourceFactoryGetter)) {
            throw new IllegalStateException("writer:"
                    + writer.getClass().getName() + " must be type of " + IDataSourceFactoryGetter.class.getName());
        }

        writerDS = ((IDataSourceFactoryGetter) writer).getDataSourceFactory();
        return writerDS;
    }

    @Override
    public File getDataXWorkDir(IPluginContext pluginContext) {

        KeyedPluginStore.KeyVal keyVal = KeyedPluginStore.AppKey.calAppName(pluginContext, this.name);
        TopologyDir topoDir = SqlTaskNodeMeta.getTopologyDir(this.name);
        File localSubFileDir = topoDir.getLocalSubFileDir();
        if (StringUtils.isEmpty(keyVal.getSuffix())) {
            return localSubFileDir;
        } else {
            return new File(localSubFileDir.getParentFile(), keyVal.getKeyVal());
        }
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
        File createDDLDir = this.getDataxCreateDDLDir(pluginCtx);
        DataxProcessor.saveCreateTableDDL(createDDL, createDDLDir, sqlFileName, overWrite);
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
        return true;
    }

    @Override
    public boolean isWriterSupportMultiTableInReader(IPluginContext pluginCtx) {
        return true;
    }

    @Override
    public DataXCfgGenerator.GenerateCfgs getDataxCfgFileNames(IPluginContext pluginCtx, Optional<JobTrigger> partialTrigger) {
        return DataxProcessor.getDataxCfgFileNames(pluginCtx, partialTrigger, this);
    }

    @Override
    public TableAliasMapper getTabAlias(IPluginContext pluginCtx, boolean withDft) {
        return TableAliasMapper.Null;
    }


    @TISExtension()
    public static class DescriptorImpl extends Descriptor<IAppSource> implements IEndTypeGetter {

        public DescriptorImpl() {
            super();
            this.registerSelectOptions(DefaultDataxProcessor.KEY_FIELD_NAME, () -> ParamsConfig.getItems(IDataxGlobalCfg.KEY_DISPLAY_NAME));
        }

        @Override
        public EndType getEndType() {
            return EndType.Workflow;
        }

        public boolean validateName(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            UploadPluginMeta pluginMeta = (UploadPluginMeta) context.get(UploadPluginMeta.KEY_PLUGIN_META);
            Objects.requireNonNull(pluginMeta, "pluginMeta can not be null");
            if (pluginMeta.isUpdate()) {
                return true;
            }
            return msgHandler.validateBizLogic(IFieldErrorHandler.BizLogic.VALIDATE_WORKFLOW_NAME_DUPLICATE, context, fieldName, value);
        }

        @Override
        public String getDisplayName() {
            return StoreResourceTypeConstants.DEFAULT_WORKFLOW_PROCESSOR_NAME;
        }
    }
}
