package com.qlangtech.tis.plugin.ontology.graphrag;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/2
 */
public class DefaultGraphRAGServiceITTest {

    @Test
    public void testRetrieve() {
        DefaultGraphRAGService graphRAGService = DefaultGraphRAGService.getInstance();
        RetrievalResult retrievalResult = graphRAGService.retrieve("falcon_14", "怎样获取Commercial区域库存量前十的产品及其当前库存量？", null);
        Assert.assertNotNull(retrievalResult);


    }
}
