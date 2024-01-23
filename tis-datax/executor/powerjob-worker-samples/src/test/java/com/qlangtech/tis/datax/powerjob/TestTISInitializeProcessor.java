package com.qlangtech.tis.datax.powerjob;

import com.qlangtech.tis.datax.DataXJobRunEnvironmentParamsSetter.ExtraJavaSystemPramsSuppiler;
import junit.framework.TestCase;
import tech.powerjob.worker.core.processor.TaskContext;
import tech.powerjob.worker.core.processor.WorkflowContext;
import tech.powerjob.worker.log.impl.OmsNullLogger;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/12/12
 */
public class TestTISInitializeProcessor extends TestCase {

    public void testProcess() throws Exception {

        ExtraJavaSystemPramsSuppiler sysPramsSuppiler = TISTableDumpProcessor.createSysPramsSuppiler();

        for (String param : sysPramsSuppiler.get()) {
            System.out.println(param);
        }

        TISInitializeProcessor initializeProcessor = new TISInitializeProcessor();

        TaskContext context = new TaskContext();

        context.setWorkflowContext(new WorkflowContext(1l, "{}"));
        context.setOmsLogger(new OmsNullLogger());
        context.setJobParams("{ \"tisWorkflowChannel\":false, \"dataxName\":\"mysql_hive3\" }");

        initializeProcessor.process(context);
    }
}
