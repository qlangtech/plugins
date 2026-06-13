package com.qlangtech.tis.plugin.ontology.impl.linker;

import com.qlangtech.tis.plugin.datax.transformer.UDFDesc;
import com.qlangtech.tis.plugin.ontology.impl.infer.InferOntologyFromLLMStep1;
import com.qlangtech.tis.trigger.util.JsonUtil;
import org.apache.commons.collections.CollectionUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

import static com.qlangtech.tis.plugin.ontology.impl.infer.InferOntologyFromLLMStep1.initPluginContext;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/11
 */
public class RelationshipTypeBackingObjectTypeTest {

    @Test
    public void testGetLiteria() {
        initPluginContext(InferOntologyFromLLMStep1.ontologyName);
        RelationshipTypeBackingObjectType backingObjectType = new RelationshipTypeBackingObjectType();
        backingObjectType.leftObjectType = "toy_stores";
        backingObjectType.rightObjectType = "toy_products";
        JoinReference joinRef = new JoinReference();
        joinRef.rightTargetField = "Product_ID";
        joinRef.targetField = "Store_ID";
        joinRef.objectType = "toy_inventory";
        backingObjectType.joinObjectType = joinRef;

        List<UDFDesc> literia = backingObjectType.getLiteria();
        Assert.assertTrue(CollectionUtils.isNotEmpty(literia));
        System.out.println(JsonUtil.toString(literia, true));
    }
}