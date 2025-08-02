package com.qlangtech.tis.datax.powerjob;

import com.qlangtech.tis.datax.executor.BasicTISInitializeProcessor;
import com.qlangtech.tis.datax.powerjob.impl.PowerJobTaskContext;
import tech.powerjob.worker.core.processor.ProcessResult;
import tech.powerjob.worker.core.processor.TaskContext;
import tech.powerjob.worker.core.processor.sdk.BasicProcessor;

/**
 * 所有子节点都要依赖该节点，如果发现是定时任务启动，则需要初始化TaskId等信息，提供给下游节点使用
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/11/26
 */
public class TISInitializeProcessor extends BasicTISInitializeProcessor implements BasicProcessor {
    @Override
    public ProcessResult process(TaskContext context) throws Exception {

        this.initializeProcess(new PowerJobTaskContext(context));
        return new ProcessResult(true);
    }


}
