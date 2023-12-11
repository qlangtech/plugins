package com.qlangtech.tis.datax;

import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.manage.biz.dal.pojo.Application;
import com.qlangtech.tis.plugin.StoreResourceType;
import junit.framework.TestCase;

import java.util.Objects;

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
            public IDataxGlobalCfg getDataXGlobalCfg() {
                return null;
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
