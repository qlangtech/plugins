package com.qlangtech.tis.datax;

import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.datax.impl.TransformerInfo;
import com.qlangtech.tis.manage.biz.dal.pojo.Application;
import com.qlangtech.tis.datax.StoreResourceType;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.util.IPluginContext;
import junit.framework.TestCase;
import org.apache.commons.lang3.tuple.Pair;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/11/20
 */
public class TestDataxPrePostConsumer extends TestCase {

    String tableName = "totalpayinfo";
    Integer taskId = 1;
    String dataXName = "mysql_hive3";
    Long currentTimeStamp = TimeFormat.getCurrentTimeStamp();

    public void testConsumePreExecMessage() throws Exception {


        DataxPrePostConsumer prePostConsumer = new DataxPrePostConsumer();

        DataXLifecycleHookMsg lifecycleHookMsg = createHookMsg(IDataXBatchPost.LifeCycleHook.Prep);

        prePostConsumer.consumeMessage(lifecycleHookMsg);
    }


    private DataXLifecycleHookMsg createHookMsg(IDataXBatchPost.LifeCycleHook lifeCycleHook) {

        IDataxProcessor processor = new DataxProcessor() {
            @Override
            public Application buildApp() {
                return null;
            }

            @Override
            public Pair<List<RecordTransformerRules>, IPluginStore> getRecordTransformerRulesAndPluginStore(IPluginContext pluginCtx, String tableName) {
                return null;
            }

            @Override
            public IDataxReader getReader(IPluginContext pluginContext, ISelectedTab tab) {
                return null;
            }

            @Override
            public IDataxGlobalCfg getDataXGlobalCfg() {
                return null;
            }

            @Override
            public Set<TransformerInfo> getTransformerInfo(IPluginContext pluginCtx, Map<String, List<DBDataXChildTask>> groupedChildTask) {
                return Set.of();
            }

            @Override
            public String identityValue() {
                return dataXName;
            }

            @Override
            public StoreResourceType getResType() {
                return StoreResourceType.DataApp;
            }
        };

        Objects.requireNonNull(lifeCycleHook, "lifeCycleHook can not be null");
        return DataXLifecycleHookMsg.createDataXLifecycleHookMsg(processor, tableName, taskId,
                IDataXBatchPost.KEY_PREP + tableName, currentTimeStamp, lifeCycleHook, false);
    }

    public void testConsumePostExecMessage() throws Exception {
        DataxPrePostConsumer prePostConsumer = new DataxPrePostConsumer();

        DataXLifecycleHookMsg lifecycleHookMsg = createHookMsg(IDataXBatchPost.LifeCycleHook.Post);

        prePostConsumer.consumeMessage(lifecycleHookMsg);
    }

}
