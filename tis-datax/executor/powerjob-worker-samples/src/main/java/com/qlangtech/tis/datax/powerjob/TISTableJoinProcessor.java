package com.qlangtech.tis.datax.powerjob;

import com.qlangtech.tis.datax.executor.BasicTISTableJoinProcessor;
import com.qlangtech.tis.datax.powerjob.impl.PowerJobTaskContext;
import tech.powerjob.worker.core.processor.ProcessResult;
import tech.powerjob.worker.core.processor.TaskContext;
import tech.powerjob.worker.core.processor.sdk.BasicProcessor;

import static com.qlangtech.tis.datax.powerjob.TISTableDumpProcessor.setDataXExecutorDir;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/11/11
 */
public class TISTableJoinProcessor extends BasicTISTableJoinProcessor implements BasicProcessor {
    static {
        setDataXExecutorDir();
    }

    @Override
    public ProcessResult process(TaskContext context) throws Exception {

        this.process(new PowerJobTaskContext(context));
        return new ProcessResult(true);
    }

}
