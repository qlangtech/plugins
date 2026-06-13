package com.qlangtech.tis.plugin.ontology.impl.infer;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.datax.job.SSEEventWriter;
import com.qlangtech.tis.datax.job.SSERunnable;
import com.qlangtech.tis.extension.OneStepOfMultiSteps;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.trigger.util.JsonUtil;
import com.qlangtech.tis.util.IPluginContext;
import com.qlangtech.tis.util.UploadPluginMeta;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static com.qlangtech.tis.datax.job.SSERunnable.SSEEventType.AI_AGNET_DONE;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/11
 */

public abstract class BasicInfterExecuteDesc extends OneStepOfMultiSteps.BasicDesc {


    @Override
    public final void httpProcess(IControlMsgHandler paramGetter, IPluginContext pluginContext, Context context) throws Exception {
        List<UploadPluginMeta> metas = pluginContext.getPluginMeta();
        for (UploadPluginMeta meta : metas) {
            UploadPluginMeta.putPluginMeta(context, meta);
            break;
        }
        DeserializeOntologyRes.InferBatch inferBatch = getInferBatch();
        DeserializeOntologyRes ontologyRes
                = DeserializeOntologyRes.getDomainInferResult(OntologyPluginMeta.createPluginMeta().getDomain());
        final String type = paramGetter.getString("type");

        if ("getResLiteria".equals(type)) {
            Integer id = Integer.parseInt(paramGetter.getString("id"));
            pluginContext.setBizResult(context, ontologyRes.findInferenceById(id).getOntologyLiteriaInfo());
            return;
        }

        if ("stop".equals(type)) {
            ontologyRes.stopInferTask(inferBatch);
            return;
        }
        if ("resume".equals(type)) {
            ontologyRes.stopInferTask(inferBatch);
            ontologyRes.clearDomainQueues(inferBatch);
            // fall through — 继续向下执行推理
            // DeserializeOntologyRes.getOntologyResInfer(ometa.getDomain(),pluginContext,ctx,this,step1);
            return;
        }


        SSEEventWriter sseWriter = paramGetter.getEventStreamWriter();
        CountDownLatch countDown = new CountDownLatch(1);
        InferenceParseSubscriber inferSubscriber = new InferenceParseSubscriber(inferBatch) {
            @Override
            public void onNext(InferenceParse item) {
                sseWriter.writeSSEEvent(SSERunnable.SSEEventType.LLM_ONTOLOGY_REFER_RECORD, JsonUtil.toString(item, false));
            }

            @Override
            public void onComplete() {
                countDown.countDown();
            }
        };
        ontologyRes.subscribe(inferSubscriber);

        if (countDown.await(20, TimeUnit.MINUTES)) {
            // 正常结束
        }

        sseWriter.writeSSEEvent(AI_AGNET_DONE, String.valueOf(true));
    }

    protected abstract DeserializeOntologyRes.InferBatch getInferBatch();




}
