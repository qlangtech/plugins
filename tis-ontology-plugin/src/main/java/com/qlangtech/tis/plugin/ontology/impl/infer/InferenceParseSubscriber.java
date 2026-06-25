package com.qlangtech.tis.plugin.ontology.impl.infer;

import java.util.concurrent.Flow;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/9
 */
@SuppressWarnings("all")
public class InferenceParseSubscriber implements Flow.Subscriber<InferenceParse> {
    private Flow.Subscription subscription;
    private final DeserializeOntologyRes.InferBatch inferBatch;

    public InferenceParseSubscriber(DeserializeOntologyRes.InferBatch inferBatch) {
        this.inferBatch = inferBatch;
    }

    public DeserializeOntologyRes.InferBatch getInferBatch() {
        return inferBatch;
    }

    @Override
    public void onSubscribe(Flow.Subscription subscription) {
        this.subscription = subscription;
        subscription.request(Long.MAX_VALUE);
    }

    @Override
    public void onNext(InferenceParse item) {

    }

    @Override
    public void onError(Throwable throwable) {

    }

    @Override
    public void onComplete() {

    }
}
