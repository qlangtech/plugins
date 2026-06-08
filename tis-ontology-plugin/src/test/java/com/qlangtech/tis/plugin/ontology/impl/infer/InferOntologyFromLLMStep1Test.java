package com.qlangtech.tis.plugin.ontology.impl.infer;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.citrus.turbine.impl.DefaultContext;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.aiagent.llm.ITISJsonSchema;
import com.qlangtech.tis.plugin.ontology.OntologySharedProperty;
import com.qlangtech.tis.trigger.util.JsonUtil;
import com.qlangtech.tis.util.IPluginContext;
import com.qlangtech.tis.util.PartialSettedPluginContext;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;

import static com.qlangtech.tis.plugin.ontology.impl.infer.InferOntologyFromLLMStep1.deserializeElement;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/5
 */
public class InferOntologyFromLLMStep1Test {

    @Test
    public void testBuildLinkTypeItemSchema() {
        InferOntologyFromLLMStep1 inferOntologyFromLLMStep1 = new InferOntologyFromLLMStep1();

        ITISJsonSchema linkSchema = inferOntologyFromLLMStep1.buildLinkTypeItemSchema();

        System.out.println(JsonUtil.toString(linkSchema.root(), true));
    }

    @Test
    public void testOnValueTypeDeserialize() {
        /**
         * constraint 字段 FlatJsonToTisConverter.convert(json) 处理后需要变成如下格式
         * <pre>
         * "constraint": {
         * 					"descVal": {
         * 						"impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.constraints.Enum4String",
         * 						"vals": {
         * 							"enumVals": {
         * 								"_eprops": {
         * 									"enum": {
         * 										"mcols": [],
         * 										"typeMetas": [],
         * 										"_mcols": [{
         * 											"enumVal": "1"
         * 								             }, {
         * 											"enumVal": "2"
         *                                        }]}
         * 								}
         * 							},
         * 							"caseInsensitive": {
         * 								"_primaryVal": true
         * 							}
         * 						}
         * 					}
         * 				}
         *
         * </pre>
         */
        JSONObject valType = JSONObject.parseObject("""
                {
                	"impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.DefaultOntologyValueType",
                	"reason": "payinfo_extra.settlement 注释包含枚举值列表",
                	"targetColumns": [{
                		"column": "settlement",
                		"table": "payinfo_extra"
                	}],
                	"vals": {
                		"Step2": {
                			"impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.ConstraintsOfValueType",
                			"vals": {
                				"constraint": {
                					"enumVals": ["0", "1", "2"],
                					"caseInsensitive": true,
                					"$id": "com.qlangtech.tis.plugin.ontology.impl.valuetype.constraints.Enum4String"
                				}
                			}
                		},
                		"Step1": {
                			"impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.MetadataOfValueType",
                			"vals": {
                				"name": "SettlementStatus",
                				"description": "确认支付状态：(0无需确认,1待确认,2已确认)",
                				"type": 3
                			}
                		}
                	},
                	"confidence": "high"
                }
                """);
        PartialSettedPluginContext pluginContext = IPluginContext.namedContext("test");
        Context context = new DefaultContext();
        pluginContext.setContext(context);
        Pair<OntologySharedProperty, InferenceParse> result = deserializeElement(valType, pluginContext, context);
        Assert.assertNotNull(result);
    }
}