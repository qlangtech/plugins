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

package com.qlangtech.tis.datax.executor;

import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.qlangtech.tis.datax.CuratorDataXTaskMessage;
import com.qlangtech.tis.datax.DBDataXChildTask;
import com.qlangtech.tis.datax.DataXJobInfo;
import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxGlobalCfg;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxProcessor.TableMap;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.datax.IDataxWriter;
import com.qlangtech.tis.datax.StoreResourceType;
import com.qlangtech.tis.datax.TableAliasMapper;
import com.qlangtech.tis.datax.impl.DataXCfgGenerator.GenerateCfgs;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.datax.impl.DataxProcessor.IDataxProcessorGetter;
import com.qlangtech.tis.datax.impl.DataxWriter.BaseDataxWriterDescriptor;
import com.qlangtech.tis.datax.impl.TransformerInfo;
import com.qlangtech.tis.datax.powerjob.CfgsSnapshotConsumer;
import com.qlangtech.tis.datax.powerjob.ExecPhase;
import com.qlangtech.tis.exec.AbstractExecContext;
import com.qlangtech.tis.exec.IExecChainContext;
import com.qlangtech.tis.job.common.JobParams;
import com.qlangtech.tis.manage.biz.dal.pojo.Application;
import com.qlangtech.tis.manage.common.CenterResource;
import com.qlangtech.tis.offline.DataxUtils;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.PluginAndCfgsSnapshotUtils;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.DefaultTab;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.trigger.JobTrigger;
import com.qlangtech.tis.powerjob.SelectedTabTriggers;
import com.qlangtech.tis.powerjob.SelectedTabTriggers.PowerJobRemoteTaskTrigger;
import com.qlangtech.tis.powerjob.SelectedTabTriggersConfig;
import com.qlangtech.tis.test.TISEasyMock;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.lang3.tuple.Triple;
import org.easymock.EasyMock;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.junit.Assert.*;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-08-11 22:23
 **/
public class BasicTISTableDumpProcessorTest implements TISEasyMock {

    @Before
    public void setUp() throws Exception {
        this.clearMocks();
    }

    @Test
    public void createExecContext() {
        CenterResource.setNotFetchFromCenterRepository();
        ITaskExecutorContext context = mock("context", ITaskExecutorContext.class);
        EasyMock.expect(context.getSpecifiedLocalLoggerPath()).andReturn(new File("/opt/data/logs"));
        EasyMock.expect(context.isDisableGrpcRemoteServerConnect()).andReturn(true);
        String pipeName = "pipename";
        Integer taskId = 999;
        StoreResourceType type = StoreResourceType.DataApp;
        // JobParams.KEY_TASK_ID
        IDataxWriter writer = mockWriter();
        // writer.startScanDependency();
        // EasyMock.expectLastCall().anyTimes();
        DataxProcessor mockApp = mockAppSource(pipeName, writer, type);
        DataxProcessor.processorGetter = (name) -> {
            return mockApp;
        };
        JSONObject instanceParams = IExecChainContext.createInstanceParams(taskId, mockApp, false, Optional.empty());
//        instanceParams.put(DataxUtils.EXEC_TIMESTAMP, DataxUtils.currentTimeStamp());
//        instanceParams.put(JobParams.KEY_COLLECTION, pipeName);
//        instanceParams.put(PluginAndCfgsSnapshotUtils.KEY_PLUGIN_CFGS_METAS, "{}");
        //instanceParams.put(DataxUtils., DataxUtils.currentTimeStamp());
        ISelectedTab tab = new DefaultTab("base");
        IDataxProcessor appSource = mock("appSource", IDataxProcessor.class);
//        EasyMock.expect(appSource.getResType()).andReturn(type);
//        EasyMock.expect(appSource.createNode()).andReturn(IDataxProcessor.createNode(pipeName, type));
        EasyMock.expect(context.getInstanceParams()).andReturn(instanceParams);

        SelectedTabTriggers tabTriggers = new SelectedTabTriggers(tab, mockApp);
        DataXJobInfo dataXJobInfo = DataXJobInfo.parse("base_1.json/base_01,base_02");

        CuratorDataXTaskMessage tskMsg = new CuratorDataXTaskMessage();
        tskMsg.setDataXName(pipeName);
        tskMsg.setResType(type);
        PowerJobRemoteTaskTrigger taskTrigger = new PowerJobRemoteTaskTrigger(dataXJobInfo, tskMsg);

        tabTriggers.setSplitTabTriggers(Collections.singletonList(taskTrigger));
        JSONObject jobParams = tabTriggers.createMRParams();
        EasyMock.expect(context.getJobParams()).andReturn(jobParams);


        instanceParams.put(JobParams.KEY_TASK_ID, taskId);

        replay();
        Triple<AbstractExecContext, CfgsSnapshotConsumer, SelectedTabTriggersConfig> result
                = BasicTISTableDumpProcessor.createExecContext(context, ExecPhase.Reduce);

        AbstractExecContext execContext = result.getLeft();
        Assert.assertEquals(taskId, (Integer) execContext.getTaskId());

        verifyAll();
    }

    private IDataxWriter mockWriter() {
        return new IDataxWriter() {
            @Override
            public String getTemplate() {
                return "";
            }

            @Override
            public BaseDataxWriterDescriptor getWriterDescriptor() {
                return null;
            }

            @Override
            public IDataxContext getSubTask(Optional<TableMap> tableMap, Optional<RecordTransformerRules> transformerRules) {
                return null;
            }

            @Override
            public void startScanDependency() {

            }
        };
    }

    private static DataxProcessor mockAppSource(String pipeName, IDataxWriter writer, StoreResourceType type) {
        return new DataxProcessor() {
            @Override
            public String identityValue() {
                return pipeName;
            }

            @Override
            public StoreResourceType getResType() {
                return StoreResourceType.DataApp;
            }

            @Override
            public JSONObject createNode() {
                return IDataxProcessor.createNode(pipeName, type);
            }

            @Override
            public Pair<List<RecordTransformerRules>, IPluginStore> getRecordTransformerRulesAndPluginStore(IPluginContext pluginCtx, String tableName) {
                return null;
            }

            @Override
            public List<IDataxReader> getReaders(IPluginContext pluginCtx) {
                return Collections.emptyList();
            }

            @Override
            public IDataxReader getReader(IPluginContext pluginContext, ISelectedTab tab) {
                return null;
            }

            @Override
            public IDataxReader getReader(IPluginContext pluginCtx) {
                return null;
            }

            @Override
            public IDataxWriter getWriter(IPluginContext pluginCtx, boolean validateNull) {
                return writer;
            }

            @Override
            public IDataxGlobalCfg getDataXGlobalCfg() {
                return null;
            }

            @Override
            public File getDataxCfgDir(IPluginContext pluginCtx) {
                return null;
            }

            @Override
            public File getDataxCreateDDLDir(IPluginContext pluginContext) {
                return null;
            }

            @Override
            public void saveCreateTableDDL(IPluginContext pluginCtx, StringBuffer createDDL, String sqlFileName, boolean overWrite) throws IOException {

            }

            @Override
            public File getDataXWorkDir(IPluginContext pluginContext) {
                return null;
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
            public GenerateCfgs getDataxCfgFileNames(IPluginContext pluginCtx, Optional<JobTrigger> partialTrigger) {
                return null;
            }

            @Override
            public Application buildApp() {
                return null;
            }

            @Override
            public TableAliasMapper getTabAlias(IPluginContext pluginCtx) {
                return null;
            }

            @Override
            public Set<TransformerInfo> getTransformerInfo(IPluginContext pluginCtx, Map<String, List<DBDataXChildTask>> groupedChildTask) {
                return Set.of();
            }
        };
    }


}