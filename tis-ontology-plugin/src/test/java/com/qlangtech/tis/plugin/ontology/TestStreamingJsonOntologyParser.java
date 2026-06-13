/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qlangtech.tis.plugin.ontology;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.StringUtils;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

/**
 * StreamingJsonOntologyParser 的单元测试
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/7
 */
public class TestStreamingJsonOntologyParser {


    @Test
    public void testRealStreaming2() throws IOException {
        String jsonContent =
                """
                {
                  "ontology-glossary": [
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "表名 'queueop' 存储排队操作历史，对应业务实体 '排队操作'。从表名直接提取。",
                      "vals": {
                        "synonyms": ["queue operation", "排队记录", "排号操作"],
                        "description": "排队操作记录",
                        "term": "queueop",
                        "target": {
                          "objectType": "queueop",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetOT"
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "列注释 '(1.开始排队,2.停止排队,3.取号,4.叫号,5.过号,6.取消排队(系统),7.取消排队(火小二))' 明确列举了操作类型枚举值。",
                      "vals": {
                        "synonyms": ["operation type", "操作类别", "排队动作"],
                        "description": "排队操作的类型",
                        "term": "op_type",
                        "target": {
                          "objectType": "queueop",
                          "targetField": "op_type",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetProperty"
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "表名 'sign_flow_task' 存储签章流程任务，对应业务实体。",
                      "vals": {
                        "synonyms": ["sign task", "签章任务", "签署流程"],
                        "description": "签章流程任务记录",
                        "term": "sign_flow_task",
                        "target": {
                          "objectType": "sign_flow_task",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetOT"
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "表名 'queuestatus' 表示排队状态，对应业务实体。",
                      "vals": {
                        "synonyms": ["queue status", "排队情形"],
                        "description": "排队状态信息",
                        "term": "queuestatus",
                        "target": {
                          "objectType": "queuestatus",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetOT"
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "列注释 '状态：(1.开始排队,2停止排队)' 明确枚举值。",
                      "vals": {
                        "synonyms": ["状态", "排队状态", "queue state"],
                        "description": "排队的状态",
                        "term": "status",
                        "target": {
                          "objectType": "queuestatus",
                          "targetField": "status",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetProperty"
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "表名 'instance_asset' 存储实例资产，对应业务实体。",
                      "vals": {
                        "synonyms": ["asset", "实例资产", "权益"],
                        "description": "实例资产信息",
                        "term": "instance_asset",
                        "target": {
                          "objectType": "instance_asset",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetOT"
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "列注释 '资产状态1、未处理，2、资产交付' 明确枚举值。",
                      "vals": {
                        "synonyms": ["asset status", "权益状态", "交付状态"],
                        "description": "资产的处理状态",
                        "term": "asset_status",
                        "target": {
                          "objectType": "instance_asset",
                          "targetField": "asset_status",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetProperty"
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "表名 'payinfo_extra' 是支付额外信息，对应业务实体。",
                      "vals": {
                        "synonyms": ["payment extra", "额外支付信息"],
                        "description": "支付方式的额外信息",
                        "term": "payinfo_extra",
                        "target": {
                          "objectType": "payinfo_extra",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetOT"
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "列注释 '确认支付，0:无需确认，1：待确认，2:已确认' 明确枚举值。",
                      "vals": {
                        "synonyms": ["settlement status", "结算确认", "确认支付"],
                        "description": "支付确认状态",
                        "term": "settlement",
                        "target": {
                          "objectType": "payinfo_extra",
                          "targetField": "settlement",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetProperty"
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "表名 'waitingorderdetail' 对应预订单详情。",
                      "vals": {
                        "synonyms": ["waiting order detail", "预订单详情", "排单信息"],
                        "description": "预订单详细信息",
                        "term": "waitingorderdetail",
                        "target": {
                          "objectType": "waitingorderdetail",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetOT"
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "列注释 '订单来源：1/淘宝点点;2/卡包；3/服务生app；4/微信' 明确枚举。",
                      "vals": {
                        "synonyms": ["order source", "来源", "下单渠道"],
                        "description": "订单的来源渠道",
                        "term": "order_from",
                        "target": {
                          "objectType": "waitingorderdetail",
                          "targetField": "order_from",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetProperty"
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "表名 'orderdetail' 存储订单详情。",
                      "vals": {
                        "synonyms": ["order detail", "订单信息", "点单明细"],
                        "description": "订单详细信息",
                        "term": "orderdetail",
                        "target": {
                          "objectType": "orderdetail",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetOT"
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "列注释 '1正常 2并单 3撤消 4结账' 明确订单状态枚举。",
                      "vals": {
                        "synonyms": ["order status", "订单状态", "状态"],
                        "description": "订单的状态",
                        "term": "order_status",
                        "target": {
                          "objectType": "orderdetail",
                          "targetField": "status",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetProperty"
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "表名 'servicebillinfo' 对应服务账单信息。",
                      "vals": {
                        "synonyms": ["service bill", "服务账单", "账单"],
                        "description": "服务账单信息",
                        "term": "servicebillinfo",
                        "target": {
                          "objectType": "servicebillinfo",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetOT"
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "表名 'order_refund' 存储订单退款信息。",
                      "vals": {
                        "synonyms": ["refund", "退款", "退单"],
                        "description": "订单退款信息",
                        "term": "order_refund",
                        "target": {
                          "objectType": "order_refund",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetOT"
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "列注释 '处理状态（ 1 处理中 ，2,失败,3, 完成 ,4 异常 , 5 撤销）' 明确枚举。",
                      "vals": {
                        "synonyms": ["refund status", "退款状态", "处理情况"],
                        "description": "退款的处理状态",
                        "term": "refund_status",
                        "target": {
                          "objectType": "order_refund",
                          "targetField": "status",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetProperty"
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "表名 'order_promotion' 存储订单优惠信息。",
                      "vals": {
                        "synonyms": ["order promotion", "优惠", "折扣"],
                        "description": "订单优惠详情",
                        "term": "order_promotion",
                        "target": {
                          "objectType": "order_promotion",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetOT"
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "表名 'order_snapshot' 存储订单快照。",
                      "vals": {
                        "synonyms": ["order snapshot", "订单快照", "支付快照"],
                        "description": "订单支付快照信息",
                        "term": "order_snapshot",
                        "target": {
                          "objectType": "order_snapshot",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetOT"
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "表名 'promotion' 存储优惠活动。",
                      "vals": {
                        "synonyms": ["promotion", "优惠活动", "促销"],
                        "description": "优惠活动信息",
                        "term": "promotion",
                        "target": {
                          "objectType": "promotion",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetOT"
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "表名 'user' 存储用户信息。",
                      "vals": {
                        "synonyms": ["user", "用户", "顾客", "customer"],
                        "description": "用户账号信息",
                        "term": "user",
                        "target": {
                          "objectType": "user",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetOT"
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "表名 'refund_pay_item' 存储退款支付项明细。",
                      "vals": {
                        "synonyms": ["refund pay item", "退款支付项", "退款明细"],
                        "description": "退款支付项信息",
                        "term": "refund_pay_item",
                        "target": {
                          "objectType": "refund_pay_item",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetOT"
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "表名 'order_tag' 存储订单标签。",
                      "vals": {
                        "synonyms": ["order tag", "标签", "订单标记"],
                        "description": "订单标签信息",
                        "term": "order_tag",
                        "target": {
                          "objectType": "order_tag",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetOT"
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "表名 'specialfee' 存储特殊费用。",
                      "vals": {
                        "synonyms": ["special fee", "特殊费用", "额外费用"],
                        "description": "特殊费用信息",
                        "term": "specialfee",
                        "target": {
                          "objectType": "specialfee",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetOT"
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "表名 'order_bill' 存储订单账单。",
                      "vals": {
                        "synonyms": ["order bill", "账单", "支付单"],
                        "description": "订单账单信息",
                        "term": "order_bill",
                        "target": {
                          "objectType": "order_bill",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetOT"
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "表名 'presell_order_extra' 对应预售订单扩展信息。",
                      "vals": {
                        "synonyms": ["presell order extra", "预售扩展", "预定扩展"],
                        "description": "预售订单扩展信息",
                        "term": "presell_order_extra",
                        "target": {
                          "objectType": "presell_order_extra",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetOT"
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "表名 'grid_field' 为配置表，存储表格字段配置。",
                      "vals": {
                        "synonyms": ["grid field", "报表字段", "字段配置"],
                        "description": "报表字段配置信息",
                        "term": "grid_field",
                        "target": {
                          "objectType": "grid_field",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetOT"
                        }
                      },
                      "confidence": "medium"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "表名 'discount_detail' 存储折扣明细。",
                      "vals": {
                        "synonyms": ["discount detail", "折扣明细", "优惠明细"],
                        "description": "菜品折扣明细信息",
                        "term": "discount_detail",
                        "target": {
                          "objectType": "discount_detail",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetOT"
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "表名 'waitingordercrid' 存储预订单与用户关系。",
                      "vals": {
                        "synonyms": ["waiting order customer relation", "预订单顾客关联"],
                        "description": "预订单与顾客关联关系",
                        "term": "waitingordercrid",
                        "target": {
                          "objectType": "waitingordercrid",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetOT"
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "表名 'customer_order_relation' 存储顾客订单关系。",
                      "vals": {
                        "synonyms": ["customer order relation", "顾客订单关联"],
                        "description": "顾客与订单的关联关系",
                        "term": "customer_order_relation",
                        "target": {
                          "objectType": "customer_order_relation",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetOT"
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "表名 'waiting_pay' 存储预支付信息。",
                      "vals": {
                        "synonyms": ["waiting pay", "待支付", "预支付"],
                        "description": "预支付信息",
                        "term": "waiting_pay",
                        "target": {
                          "objectType": "waiting_pay",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetOT"
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "表名 'waitinginstanceinfo' 对应预点菜信息。",
                      "vals": {
                        "synonyms": ["waiting instance", "预点菜", "待点菜"],
                        "description": "预点菜明细信息",
                        "term": "waitinginstanceinfo",
                        "target": {
                          "objectType": "waitinginstanceinfo",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetOT"
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "表名 'takeout_order_extra' 存储外卖订单扩展信息。",
                      "vals": {
                        "synonyms": ["takeout order extra", "外卖扩展", "外送扩展"],
                        "description": "外卖订单扩展信息",
                        "term": "takeout_order_extra",
                        "target": {
                          "objectType": "takeout_order_extra",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetOT"
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "表名 'payinfo' 存储支付明细。",
                      "vals": {
                        "synonyms": ["payment info", "支付信息", "付款记录"],
                        "description": "支付明细信息",
                        "term": "payinfo",
                        "target": {
                          "objectType": "payinfo",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetOT"
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "表名 'paydetail' 存储支付额外信息。",
                      "vals": {
                        "synonyms": ["payment detail", "支付额外信息", "付款详情"],
                        "description": "支付方式的额外详情",
                        "term": "paydetail",
                        "target": {
                          "objectType": "paydetail",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetOT"
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "表名 'instancedetail' 对应订单菜品实例详情。",
                      "vals": {
                        "synonyms": ["instance detail", "菜品明细", "点菜详情"],
                        "description": "订单菜品实例详细信息",
                        "term": "instancedetail",
                        "target": {
                          "objectType": "instancedetail",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetOT"
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "表名 'totalpayinfo' 存储总支付账单信息。",
                      "vals": {
                        "synonyms": ["total pay info", "总账单", "支付汇总"],
                        "description": "总支付账单信息",
                        "term": "totalpayinfo",
                        "target": {
                          "objectType": "totalpayinfo",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetOT"
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "列名 'fee' 出现在多张表中表达金额，属于通用业务字段。",
                      "vals": {
                        "synonyms": ["amount", "金额", "费用", "数额"],
                        "description": "金额字段",
                        "term": "fee",
                        "target": {
                          "objectType": "servicebillinfo",
                          "targetField": "fee",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetProperty"
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "列 'order_id' 在众多表中作为外键引用订单，属于核心属性。",
                      "vals": {
                        "synonyms": ["order ID", "订单编号", "订单号"],
                        "description": "订单标识",
                        "term": "order_id",
                        "target": {
                          "objectType": "orderdetail",
                          "targetField": "order_id",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetProperty"
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "列 'entity_id' 在所有表中出现，注释为店铺ID或所属实体，是核心业务实体标识。",
                      "vals": {
                        "synonyms": ["store ID", "店铺ID", "餐厅ID", "entity"],
                        "description": "餐厅/店铺标识",
                        "term": "entity_id",
                        "target": {
                          "objectType": "orderdetail",
                          "targetField": "entity_id",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetProperty"
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "指标：总销售额，典型聚合指标，由订单总金额求和。",
                      "vals": {
                        "synonyms": ["总销售额", "总营收", "销售总额", "GMV"],
                        "description": "总销售额，通常为订单最终金额之和",
                        "term": "total_sales",
                        "target": {
                          "sql": "SUM(totalpayinfo.final_amount)",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetMetricExpr"
                        }
                      },
                      "confidence": "medium"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "指标：订单数，通过计数表达。",
                      "vals": {
                        "synonyms": ["订单数", "订单总量", "单量"],
                        "description": "订单总数量",
                        "term": "order_count",
                        "target": {
                          "sql": "COUNT(DISTINCT orderdetail.order_id)",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetMetricExpr"
                        }
                      },
                      "confidence": "medium"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "指标：客单价，即平均每单金额。",
                      "vals": {
                        "synonyms": ["客单价", "平均订单金额", "ASP"],
                        "description": "平均每笔订单的金额",
                        "term": "avg_order_amount",
                        "target": {
                          "sql": "AVG(totalpayinfo.final_amount)",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetMetricExpr"
                        }
                      },
                      "confidence": "medium"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "指标：退款总额，由退款实际金额求和。",
                      "vals": {
                        "synonyms": ["退款总额", "总退款", "退额"],
                        "description": "所有退款成功的总金额",
                        "term": "total_refund_amount",
                        "target": {
                          "sql": "SUM(refund_pay_item.actual_fee)",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetMetricExpr"
                        }
                      },
                      "confidence": "medium"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "指标：优惠总额，由订单优惠汇总。",
                      "vals": {
                        "synonyms": ["优惠总额", "总折扣", "促销金额"],
                        "description": "所有订单的优惠金额总和",
                        "term": "total_promotion_fee",
                        "target": {
                          "sql": "SUM(order_promotion.promotion_fee)",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetMetricExpr"
                        }
                      },
                      "confidence": "medium"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                      "reason": "指标：活跃用户数，基于支付用户去重计数。",
                      "vals": {
                        "synonyms": ["活跃用户数", "支付用户数", "付费人数"],
                        "description": "有过支付行为的用户数",
                        "term": "active_users",
                        "target": {
                          "sql": "COUNT(DISTINCT waiting_pay.customer_register_id)",
                          "$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetMetricExpr"
                        }
                      },
                      "confidence": "medium"
                    }
                  ],
                  "ontology-shared-property": [
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.sharedproperty.DefaultOntologySharedProperty",
                      "reason": "多张表包含 'entity_id' 字段，类型均为 STRING，且注释中均视为店铺/实体标识。",
                      "targetColumns": [
                        {"column": "entity_id", "table": "queueop"},
                        {"column": "entity_id", "table": "queuestatus"},
                        {"column": "entity_id", "table": "instance_asset"},
                        {"column": "entity_id", "table": "payinfo_extra"},
                        {"column": "entity_id", "table": "waitingorderdetail"},
                        {"column": "entity_id", "table": "orderdetail"},
                        {"column": "entity_id", "table": "servicebillinfo"},
                        {"column": "entity_id", "table": "order_refund"},
                        {"column": "entity_id", "table": "order_promotion"},
                        {"column": "entity_id", "table": "order_snapshot"},
                        {"column": "entity_id", "table": "promotion"},
                        {"column": "entity_id", "table": "refund_pay_item"},
                        {"column": "entity_id", "table": "order_tag"},
                        {"column": "entity_id", "table": "specialfee"},
                        {"column": "entity_id", "table": "order_bill"},
                        {"column": "entity_id", "table": "presell_order_extra"},
                        {"column": "entity_id", "table": "discount_detail"},
                        {"column": "entity_id", "table": "waitingordercrid"},
                        {"column": "entity_id", "table": "customer_order_relation"},
                        {"column": "entity_id", "table": "waiting_pay"},
                        {"column": "entity_id", "table": "waitinginstanceinfo"},
                        {"column": "entity_id", "table": "takeout_order_extra"},
                        {"column": "entity_id", "table": "payinfo"},
                        {"column": "entity_id", "table": "paydetail"},
                        {"column": "entity_id", "table": "instancedetail"},
                        {"column": "entity_id", "table": "totalpayinfo"}
                      ],
                      "vals": {
                        "name": "entity_id",
                        "description": "餐厅/店铺实体标识",
                        "alias": "store_id",
                        "type": "1"
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.sharedproperty.DefaultOntologySharedProperty",
                      "reason": "多张表包含 'create_time' 字段，类型均为 LONG 或 TIMESTAMP，语义为创建时间。",
                      "targetColumns": [
                        {"column": "create_time", "table": "queueop"},
                        {"column": "create_time", "table": "sign_flow_task"},
                        {"column": "create_time", "table": "instance_asset"},
                        {"column": "create_time", "table": "payinfo_extra"},
                        {"column": "create_time", "table": "waitingorderdetail"},
                        {"column": "create_time", "table": "orderdetail"},
                        {"column": "create_time", "table": "servicebillinfo"},
                        {"column": "create_time", "table": "order_refund"},
                        {"column": "create_time", "table": "order_promotion"},
                        {"column": "create_time", "table": "order_snapshot"},
                        {"column": "create_time", "table": "promotion"},
                        {"column": "create_time", "table": "refund_pay_item"},
                        {"column": "create_time", "table": "order_tag"},
                        {"column": "create_time", "table": "specialfee"},
                        {"column": "create_time", "table": "order_bill"},
                        {"column": "create_time", "table": "presell_order_extra"},
                        {"column": "create_time", "table": "grid_field"},
                        {"column": "create_time", "table": "discount_detail"},
                        {"column": "create_time", "table": "waitingordercrid"},
                        {"column": "create_time", "table": "customer_order_relation"},
                        {"column": "create_time", "table": "waiting_pay"},
                        {"column": "create_time", "table": "waitinginstanceinfo"},
                        {"column": "create_time", "table": "payinfo"},
                        {"column": "create_time", "table": "paydetail"},
                        {"column": "create_time", "table": "instancedetail"},
                        {"column": "create_time", "table": "totalpayinfo"}
                      ],
                      "vals": {
                        "name": "create_time",
                        "description": "记录创建时间",
                        "alias": "created_at",
                        "type": "8"
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.sharedproperty.DefaultOntologySharedProperty",
                      "reason": "多张表包含 'op_time' 字段，类型均为 LONG，语义为操作/修改时间。",
                      "targetColumns": [
                        {"column": "op_time", "table": "queueop"},
                        {"column": "op_time", "table": "instance_asset"},
                        {"column": "op_time", "table": "payinfo_extra"},
                        {"column": "op_time", "table": "waitingorderdetail"},
                        {"column": "op_time", "table": "orderdetail"},
                        {"column": "op_time", "table": "servicebillinfo"},
                        {"column": "op_time", "table": "order_refund"},
                        {"column": "op_time", "table": "order_promotion"},
                        {"column": "op_time", "table": "order_snapshot"},
                        {"column": "op_time", "table": "promotion"},
                        {"column": "op_time", "table": "refund_pay_item"},
                        {"column": "op_time", "table": "order_tag"},
                        {"column": "op_time", "table": "specialfee"},
                        {"column": "op_time", "table": "order_bill"},
                        {"column": "op_time", "table": "presell_order_extra"},
                        {"column": "op_time", "table": "grid_field"},
                        {"column": "op_time", "table": "discount_detail"},
                        {"column": "op_time", "table": "waitingordercrid"},
                        {"column": "op_time", "table": "waiting_pay"},
                        {"column": "op_time", "table": "waitinginstanceinfo"},
                        {"column": "op_time", "table": "payinfo"},
                        {"column": "op_time", "table": "paydetail"},
                        {"column": "op_time", "table": "instancedetail"},
                        {"column": "op_time", "table": "totalpayinfo"}
                      ],
                      "vals": {
                        "name": "op_time",
                        "description": "记录修改时间",
                        "alias": "updated_at",
                        "type": "8"
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.sharedproperty.DefaultOntologySharedProperty",
                      "reason": "多张表包含 'last_ver' 字段，类型均为 LONG 或 INTEGER，语义为版本号。",
                      "targetColumns": [
                        {"column": "last_ver", "table": "queueop"},
                        {"column": "last_ver", "table": "instance_asset"},
                        {"column": "last_ver", "table": "payinfo_extra"},
                        {"column": "last_ver", "table": "waitingorderdetail"},
                        {"column": "last_ver", "table": "orderdetail"},
                        {"column": "last_ver", "table": "servicebillinfo"},
                        {"column": "last_ver", "table": "order_refund"},
                        {"column": "last_ver", "table": "order_promotion"},
                        {"column": "last_ver", "table": "order_snapshot"},
                        {"column": "last_ver", "table": "refund_pay_item"},
                        {"column": "last_ver", "table": "order_tag"},
                        {"column": "last_ver", "table": "specialfee"},
                        {"column": "last_ver", "table": "order_bill"},
                        {"column": "last_ver", "table": "presell_order_extra"},
                        {"column": "last_ver", "table": "grid_field"},
                        {"column": "last_ver", "table": "discount_detail"},
                        {"column": "last_ver", "table": "waitingordercrid"},
                        {"column": "last_ver", "table": "customer_order_relation"},
                        {"column": "last_ver", "table": "waiting_pay"},
                        {"column": "last_ver", "table": "waitinginstanceinfo"},
                        {"column": "last_ver", "table": "takeout_order_extra"},
                        {"column": "last_ver", "table": "payinfo"},
                        {"column": "last_ver", "table": "paydetail"},
                        {"column": "last_ver", "table": "instancedetail"},
                        {"column": "last_ver", "table": "totalpayinfo"}
                      ],
                      "vals": {
                        "name": "last_ver",
                        "description": "记录版本号",
                        "alias": "version",
                        "type": "8"
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.sharedproperty.DefaultOntologySharedProperty",
                      "reason": "多张表包含 'is_valid' 字段，类型均为 SHORT 或 INTEGER，语义为是否有效/删除标识。",
                      "targetColumns": [
                        {"column": "is_valid", "table": "queueop"},
                        {"column": "is_valid", "table": "instance_asset"},
                        {"column": "is_valid", "table": "payinfo_extra"},
                        {"column": "is_valid", "table": "waitingorderdetail"},
                        {"column": "is_valid", "table": "orderdetail"},
                        {"column": "is_valid", "table": "servicebillinfo"},
                        {"column": "is_valid", "table": "order_refund"},
                        {"column": "is_valid", "table": "order_promotion"},
                        {"column": "is_valid", "table": "order_snapshot"},
                        {"column": "is_valid", "table": "promotion"},
                        {"column": "is_valid", "table": "refund_pay_item"},
                        {"column": "is_valid", "table": "order_tag"},
                        {"column": "is_valid", "table": "specialfee"},
                        {"column": "is_valid", "table": "order_bill"},
                        {"column": "is_valid", "table": "grid_field"},
                        {"column": "is_valid", "table": "discount_detail"},
                        {"column": "is_valid", "table": "waitingordercrid"},
                        {"column": "is_valid", "table": "waiting_pay"},
                        {"column": "is_valid", "table": "waitinginstanceinfo"},
                        {"column": "is_valid", "table": "payinfo"},
                        {"column": "is_valid", "table": "paydetail"},
                        {"column": "is_valid", "table": "instancedetail"},
                        {"column": "is_valid", "table": "totalpayinfo"}
                      ],
                      "vals": {
                        "name": "is_valid",
                        "description": "是否有效（删除标识）",
                        "alias": "valid_flag",
                        "type": "3"
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.sharedproperty.DefaultOntologySharedProperty",
                      "reason": "多张表包含 'status' 字段，类型多变但语义均为状态。",
                      "targetColumns": [
                        {"column": "status", "table": "sign_flow_task"},
                        {"column": "status", "table": "queuestatus"},
                        {"column": "status", "table": "waitingorderdetail"},
                        {"column": "status", "table": "orderdetail"},
                        {"column": "status", "table": "order_refund"},
                        {"column": "status", "table": "order_snapshot"},
                        {"column": "status", "table": "refund_pay_item"},
                        {"column": "status", "table": "order_bill"},
                        {"column": "status", "table": "waiting_pay"},
                        {"column": "status", "table": "waitinginstanceinfo"},
                        {"column": "status", "table": "instancedetail"},
                        {"column": "status", "table": "totalpayinfo"}
                      ],
                      "vals": {
                        "name": "status",
                        "description": "状态字段",
                        "alias": "status",
                        "type": "3"
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.sharedproperty.DefaultOntologySharedProperty",
                      "reason": "多张表包含 'order_id' 字段，类型均为 STRING，语义相同。",
                      "targetColumns": [
                        {"column": "order_id", "table": "instance_asset"},
                        {"column": "order_id", "table": "waitingorderdetail"},
                        {"column": "order_id", "table": "orderdetail"},
                        {"column": "order_id", "table": "order_refund"},
                        {"column": "order_id", "table": "simplecodeorder"},
                        {"column": "order_id", "table": "order_promotion"},
                        {"column": "order_id", "table": "order_snapshot"},
                        {"column": "order_id", "table": "globalcodeorder"},
                        {"column": "order_id", "table": "promotion"},
                        {"column": "order_id", "table": "refund_pay_item"},
                        {"column": "order_id", "table": "order_tag"},
                        {"column": "order_id", "table": "specialfee"},
                        {"column": "order_id", "table": "order_bill"},
                        {"column": "order_id", "table": "presell_order_extra"},
                        {"column": "order_id", "table": "discount_detail"},
                        {"column": "order_id", "table": "waiting_pay"},
                        {"column": "order_id", "table": "takeout_order_extra"},
                        {"column": "order_id", "table": "instancedetail"},
                        {"column": "order_id", "table": "totalpayinfo"}
                      ],
                      "vals": {
                        "name": "order_id",
                        "description": "订单标识",
                        "alias": "order_id",
                        "type": "1"
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.sharedproperty.DefaultOntologySharedProperty",
                      "reason": "多张表包含 'load_time' 字段，类型为 INTEGER，语义为服务器记录时间。",
                      "targetColumns": [
                        {"column": "load_time", "table": "payinfo_extra"},
                        {"column": "load_time", "table": "orderdetail"},
                        {"column": "load_time", "table": "servicebillinfo"},
                        {"column": "load_time", "table": "simplecodeorder"},
                        {"column": "load_time", "table": "order_promotion"},
                        {"column": "load_time", "table": "globalcodeorder"},
                        {"column": "load_time", "table": "specialfee"},
                        {"column": "load_time", "table": "order_bill"},
                        {"column": "load_time", "table": "discount_detail"},
                        {"column": "load_time", "table": "payinfo"},
                        {"column": "load_time", "table": "paydetail"},
                        {"column": "load_time", "table": "instancedetail"},
                        {"column": "load_time", "table": "totalpayinfo"}
                      ],
                      "vals": {
                        "name": "load_time",
                        "description": "服务器记录时间",
                        "alias": "server_time",
                        "type": "2"
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.sharedproperty.DefaultOntologySharedProperty",
                      "reason": "多张表包含 'modify_time' 字段，类型为 INTEGER，语义为服务器修改时间。",
                      "targetColumns": [
                        {"column": "modify_time", "table": "payinfo_extra"},
                        {"column": "modify_time", "table": "orderdetail"},
                        {"column": "modify_time", "table": "servicebillinfo"},
                        {"column": "modify_time", "table": "order_promotion"},
                        {"column": "modify_time", "table": "specialfee"},
                        {"column": "modify_time", "table": "order_bill"},
                        {"column": "modify_time", "table": "discount_detail"},
                        {"column": "modify_time", "table": "payinfo"},
                        {"column": "modify_time", "table": "paydetail"},
                        {"column": "modify_time", "table": "instancedetail"},
                        {"column": "modify_time", "table": "totalpayinfo"}
                      ],
                      "vals": {
                        "name": "modify_time",
                        "description": "服务器修改时间",
                        "alias": "modified_at",
                        "type": "2"
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.sharedproperty.DefaultOntologySharedProperty",
                      "reason": "多张表包含 'op_user_id' 字段，类型为 STRING，语义为操作人ID。",
                      "targetColumns": [
                        {"column": "op_user_id", "table": "queueop"},
                        {"column": "op_user_id", "table": "instance_asset"},
                        {"column": "op_user_id", "table": "orderdetail"},
                        {"column": "op_user_id", "table": "servicebillinfo"},
                        {"column": "op_user_id", "table": "order_refund"},
                        {"column": "op_user_id", "table": "order_promotion"},
                        {"column": "op_user_id", "table": "specialfee"},
                        {"column": "op_user_id", "table": "order_bill"},
                        {"column": "op_user_id", "table": "discount_detail"},
                        {"column": "op_user_id", "table": "totalpayinfo"}
                      ],
                      "vals": {
                        "name": "op_user_id",
                        "description": "操作人ID",
                        "alias": "operator_id",
                        "type": "1"
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.sharedproperty.DefaultOntologySharedProperty",
                      "reason": "多张表包含 'ext' 字段，类型为 STRING，语义为扩展字段。",
                      "targetColumns": [
                        {"column": "ext", "table": "instance_asset"},
                        {"column": "ext", "table": "waitingorderdetail"},
                        {"column": "ext", "table": "orderdetail"},
                        {"column": "ext", "table": "servicebillinfo"},
                        {"column": "ext", "table": "order_refund"},
                        {"column": "ext", "table": "order_promotion"},
                        {"column": "ext", "table": "order_snapshot"},
                        {"column": "ext", "table": "refund_pay_item"},
                        {"column": "ext", "table": "presell_order_extra"},
                        {"column": "ext", "table": "discount_detail"},
                        {"column": "ext", "table": "waiting_pay"},
                        {"column": "ext", "table": "waitinginstanceinfo"},
                        {"column": "ext", "table": "takeout_order_extra"},
                        {"column": "ext", "table": "payinfo"},
                        {"column": "ext", "table": "totalpayinfo"}
                      ],
                      "vals": {
                        "name": "ext",
                        "description": "扩展字段",
                        "alias": "ext_info",
                        "type": "1"
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.sharedproperty.DefaultOntologySharedProperty",
                      "reason": "多张表包含 'waitingorder_id' 字段，类型均为 STRING。",
                      "targetColumns": [
                        {"column": "waitingorder_id", "table": "waitingorderdetail"},
                        {"column": "waitingorder_id", "table": "order_refund"},
                        {"column": "waitingorder_id", "table": "order_snapshot"},
                        {"column": "waitingorder_id", "table": "promotion"},
                        {"column": "waitingorder_id", "table": "waitingordercrid"},
                        {"column": "waitingorder_id", "table": "customer_order_relation"},
                        {"column": "waitingorder_id", "table": "waitinginstanceinfo"}
                      ],
                      "vals": {
                        "name": "waitingorder_id",
                        "description": "预订单标识",
                        "alias": "waiting_order_id",
                        "type": "1"
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.sharedproperty.DefaultOntologySharedProperty",
                      "reason": "多张表包含 'totalpay_id' 字段，类型均为 STRING。",
                      "targetColumns": [
                        {"column": "totalpay_id", "table": "payinfo_extra"},
                        {"column": "totalpay_id", "table": "servicebillinfo"},
                        {"column": "totalpay_id", "table": "specialfee"},
                        {"column": "totalpay_id", "table": "totalpayinfo"},
                        {"column": "totalpay_id", "table": "payinfo"}
                      ],
                      "vals": {
                        "name": "totalpay_id",
                        "description": "总账单标识",
                        "alias": "total_pay_id",
                        "type": "1"
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.sharedproperty.DefaultOntologySharedProperty",
                      "reason": "多张表包含 'customerregister_id' 字段，类型均为 STRING，语义为用户ID。",
                      "targetColumns": [
                        {"column": "customerregister_id", "table": "waitingorderdetail"},
                        {"column": "customerregister_id", "table": "orderdetail"},
                        {"column": "customerregister_id", "table": "order_snapshot"},
                        {"column": "customerregister_id", "table": "waitingordercrid"},
                        {"column": "customerregister_id", "table": "customer_order_relation"}
                      ],
                      "vals": {
                        "name": "customerregister_id",
                        "description": "顾客注册标识",
                        "alias": "customer_id",
                        "type": "1"
                      },
                      "confidence": "high"
                    }
                  ],
                  "ontology-value-type": [
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.DefaultOntologyValueType",
                      "reason": "queueop.op_type 注释明确列出枚举值 '(1.开始排队,2.停止排队,3.取号,4.叫号,5.过号,6.取消排队(系统),7.取消排队(火小二))'。",
                      "targetColumns": [
                        {"column": "op_type", "table": "queueop"}
                      ],
                      "vals": {
                        "Step2": {
                          "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.ConstraintsOfValueType",
                          "vals": {
                            "constraint": {
                              "enumVals": ["1", "2", "3", "4", "5", "6", "7"],
                              "caseInsensitive": false,
                              "$id": "com.qlangtech.tis.plugin.ontology.impl.valuetype.constraints.Enum4String"
                            }
                          }
                        },
                        "Step1": {
                          "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.MetadataOfValueType",
                          "vals": {
                            "name": "QueueOpType",
                            "description": "排队操作类型",
                            "type": "1"
                          }
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.DefaultOntologyValueType",
                      "reason": "queueop.source 注释明确枚举 '(1-火排队,2-火取号,3-火小二)'。",
                      "targetColumns": [
                        {"column": "source", "table": "queueop"}
                      ],
                      "vals": {
                        "Step2": {
                          "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.ConstraintsOfValueType",
                          "vals": {
                            "constraint": {
                              "enumVals": ["1", "2", "3"],
                              "caseInsensitive": false,
                              "$id": "com.qlangtech.tis.plugin.ontology.impl.valuetype.constraints.Enum4String"
                            }
                          }
                        },
                        "Step1": {
                          "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.MetadataOfValueType",
                          "vals": {
                            "name": "QueueOpSource",
                            "description": "排队操作来源",
                            "type": "1"
                          }
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.DefaultOntologyValueType",
                      "reason": "waitingorderdetail.order_from 注释明确枚举 '1/淘宝点点;2/卡包；3/服务生app；4/微信'。",
                      "targetColumns": [
                        {"column": "order_from", "table": "waitingorderdetail"},
                        {"column": "order_from", "table": "orderdetail"}
                      ],
                      "vals": {
                        "Step2": {
                          "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.ConstraintsOfValueType",
                          "vals": {
                            "constraint": {
                              "enumVals": ["1", "2", "3", "4"],
                              "caseInsensitive": false,
                              "$id": "com.qlangtech.tis.plugin.ontology.impl.valuetype.constraints.Enum4String"
                            }
                          }
                        },
                        "Step1": {
                          "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.MetadataOfValueType",
                          "vals": {
                            "name": "OrderFrom",
                            "description": "订单来源渠道",
                            "type": "1"
                          }
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.DefaultOntologyValueType",
                      "reason": "waitingorderdetail.kind 注释明确枚举 '1/订位;2/外卖;3/扫码加菜;4/扫桌码开单'。",
                      "targetColumns": [
                        {"column": "kind", "table": "waitingorderdetail"},
                        {"column": "kind", "table": "waitingordercrid"},
                        {"column": "kind", "table": "customer_order_relation"},
                        {"column": "kind", "table": "waitinginstanceinfo"}
                      ],
                      "vals": {
                        "Step2": {
                          "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.ConstraintsOfValueType",
                          "vals": {
                            "constraint": {
                              "enumVals": ["1", "2", "3", "4"],
                              "caseInsensitive": false,
                              "$id": "com.qlangtech.tis.plugin.ontology.impl.valuetype.constraints.Enum4String"
                            }
                          }
                        },
                        "Step1": {
                          "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.MetadataOfValueType",
                          "vals": {
                            "name": "OrderKind",
                            "description": "订单类型",
                            "type": "1"
                          }
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.DefaultOntologyValueType",
                      "reason": "waitingorderdetail.status 注释包含长枚举，已明确列出。",
                      "targetColumns": [
                        {"column": "status", "table": "waitingorderdetail"}
                      ],
                      "vals": {
                        "Step2": {
                          "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.ConstraintsOfValueType",
                          "vals": {
                            "constraint": {
                              "enumVals": ["2", "3", "4", "5", "6", "7", "-1"],
                              "caseInsensitive": false,
                              "$id": "com.qlangtech.tis.plugin.ontology.impl.valuetype.constraints.Enum4String"
                            }
                          }
                        },
                        "Step1": {
                          "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.MetadataOfValueType",
                          "vals": {
                            "name": "WaitingOrderStatus",
                            "description": "预订单状态",
                            "type": "1"
                          }
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.DefaultOntologyValueType",
                      "reason": "orderdetail.status 注释 '1正常 2并单 3撤消 4结账'。",
                      "targetColumns": [
                        {"column": "status", "table": "orderdetail"}
                      ],
                      "vals": {
                        "Step2": {
                          "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.ConstraintsOfValueType",
                          "vals": {
                            "constraint": {
                              "enumVals": ["1", "2", "3", "4"],
                              "caseInsensitive": false,
                              "$id": "com.qlangtech.tis.plugin.ontology.impl.valuetype.constraints.Enum4String"
                            }
                          }
                        },
                        "Step1": {
                          "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.MetadataOfValueType",
                          "vals": {
                            "name": "OrderDetailStatus",
                            "description": "订单详情状态",
                            "type": "1"
                          }
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.DefaultOntologyValueType",
                      "reason": "order_refund.status 注释 '(1 处理中 ，2,失败,3, 完成 ,4 异常 , 5 撤销)'。",
                      "targetColumns": [
                        {"column": "status", "table": "order_refund"}
                      ],
                      "vals": {
                        "Step2": {
                          "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.ConstraintsOfValueType",
                          "vals": {
                            "constraint": {
                              "enumVals": ["1", "2", "3", "4", "5"],
                              "caseInsensitive": false,
                              "$id": "com.qlangtech.tis.plugin.ontology.impl.valuetype.constraints.Enum4String"
                            }
                          }
                        },
                        "Step1": {
                          "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.MetadataOfValueType",
                          "vals": {
                            "name": "RefundStatus",
                            "description": "退款状态",
                            "type": "1"
                          }
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.DefaultOntologyValueType",
                      "reason": "refund_pay_item.status 注释 '(1，退款中 ，2 退款状态未知 ，3退款失败 ，4 退款成功 )'。",
                      "targetColumns": [
                        {"column": "status", "table": "refund_pay_item"}
                      ],
                      "vals": {
                        "Step2": {
                          "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.ConstraintsOfValueType",
                          "vals": {
                            "constraint": {
                              "enumVals": ["1", "2", "3", "4"],
                              "caseInsensitive": false,
                              "$id": "com.qlangtech.tis.plugin.ontology.impl.valuetype.constraints.Enum4String"
                            }
                          }
                        },
                        "Step1": {
                          "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.MetadataOfValueType",
                          "vals": {
                            "name": "RefundPayItemStatus",
                            "description": "退款支付项状态",
                            "type": "1"
                          }
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.DefaultOntologyValueType",
                      "reason": "instance_asset.asset_status 注释 '资产状态1、未处理，2、资产交付'。",
                      "targetColumns": [
                        {"column": "asset_status", "table": "instance_asset"}
                      ],
                      "vals": {
                        "Step2": {
                          "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.ConstraintsOfValueType",
                          "vals": {
                            "constraint": {
                              "enumVals": ["1", "2"],
                              "caseInsensitive": false,
                              "$id": "com.qlangtech.tis.plugin.ontology.impl.valuetype.constraints.Enum4String"
                            }
                          }
                        },
                        "Step1": {
                          "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.MetadataOfValueType",
                          "vals": {
                            "name": "AssetStatus",
                            "description": "资产状态",
                            "type": "1"
                          }
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.DefaultOntologyValueType",
                      "reason": "payinfo_extra.settlement 注释 '0:无需确认，1：待确认，2:已确认'。",
                      "targetColumns": [
                        {"column": "settlement", "table": "payinfo_extra"}
                      ],
                      "vals": {
                        "Step2": {
                          "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.ConstraintsOfValueType",
                          "vals": {
                            "constraint": {
                              "enumVals": ["0", "1", "2"],
                              "caseInsensitive": false,
                              "$id": "com.qlangtech.tis.plugin.ontology.impl.valuetype.constraints.Enum4String"
                            }
                          }
                        },
                        "Step1": {
                          "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.MetadataOfValueType",
                          "vals": {
                            "name": "SettlementStatus",
                            "description": "支付确认状态",
                            "type": "1"
                          }
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.DefaultOntologyValueType",
                      "reason": "servicebillinfo.use_cash_promotion 注释 '是否是有收银优惠，0否，1是'。",
                      "targetColumns": [
                        {"column": "use_cash_promotion", "table": "servicebillinfo"},
                        {"column": "use_cash_promotion", "table": "order_bill"}
                      ],
                      "vals": {
                        "Step2": {
                          "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.ConstraintsOfValueType",
                          "vals": {
                            "constraint": {
                              "enumVals": ["0", "1"],
                              "caseInsensitive": false,
                              "$id": "com.qlangtech.tis.plugin.ontology.impl.valuetype.constraints.Enum4String"
                            }
                          }
                        },
                        "Step1": {
                          "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.MetadataOfValueType",
                          "vals": {
                            "name": "CashPromotionFlag",
                            "description": "是否收银优惠标志",
                            "type": "1"
                          }
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.DefaultOntologyValueType",
                      "reason": "waitinginstanceinfo.status 注释 '0/待发送；1/已发送待审核;2/下单超时;3/下单失败；9/下单成功'。",
                      "targetColumns": [
                        {"column": "status", "table": "waitinginstanceinfo"}
                      ],
                      "vals": {
                        "Step2": {
                          "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.ConstraintsOfValueType",
                          "vals": {
                            "constraint": {
                              "enumVals": ["0", "1", "2", "3", "9"],
                              "caseInsensitive": false,
                              "$id": "com.qlangtech.tis.plugin.ontology.impl.valuetype.constraints.Enum4String"
                            }
                          }
                        },
                        "Step1": {
                          "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.MetadataOfValueType",
                          "vals": {
                            "name": "WaitingInstanceStatus",
                            "description": "预点菜状态",
                            "type": "1"
                          }
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.DefaultOntologyValueType",
                      "reason": "instancedetail.status 注释 '1/未确认 2/正常 3/退菜标志'。",
                      "targetColumns": [
                        {"column": "status", "table": "instancedetail"}
                      ],
                      "vals": {
                        "Step2": {
                          "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.ConstraintsOfValueType",
                          "vals": {
                            "constraint": {
                              "enumVals": ["1", "2", "3"],
                              "caseInsensitive": false,
                              "$id": "com.qlangtech.tis.plugin.ontology.impl.valuetype.constraints.Enum4String"
                            }
                          }
                        },
                        "Step1": {
                          "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.MetadataOfValueType",
                          "vals": {
                            "name": "InstanceStatus",
                            "description": "菜品实例状态",
                            "type": "1"
                          }
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.DefaultOntologyValueType",
                      "reason": "specialfee.kind 注释 '额外费用信息、最低消费信息、损益信息，分别对应kind=1/2/3'。",
                      "targetColumns": [
                        {"column": "kind", "table": "specialfee"}
                      ],
                      "vals": {
                        "Step2": {
                          "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.ConstraintsOfValueType",
                          "vals": {
                            "constraint": {
                              "enumVals": ["1", "2", "3"],
                              "caseInsensitive": false,
                              "$id": "com.qlangtech.tis.plugin.ontology.impl.valuetype.constraints.Enum4String"
                            }
                          }
                        },
                        "Step1": {
                          "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.MetadataOfValueType",
                          "vals": {
                            "name": "SpecialFeeKind",
                            "description": "特殊费用类型",
                            "type": "1"
                          }
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.DefaultOntologyValueType",
                      "reason": "waiting_pay.type 注释 '支付类型约定：1/支付宝；2/快钱；3/会员卡；4/银联'。",
                      "targetColumns": [
                        {"column": "type", "table": "waiting_pay"},
                        {"column": "type", "table": "payinfo"}
                      ],
                      "vals": {
                        "Step2": {
                          "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.ConstraintsOfValueType",
                          "vals": {
                            "constraint": {
                              "enumVals": ["1", "2", "3", "4"],
                              "caseInsensitive": false,
                              "$id": "com.qlangtech.tis.plugin.ontology.impl.valuetype.constraints.Enum4String"
                            }
                          }
                        },
                        "Step1": {
                          "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.MetadataOfValueType",
                          "vals": {
                            "name": "PaymentType",
                            "description": "支付类型",
                            "type": "1"
                          }
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.DefaultOntologyValueType",
                      "reason": "order_refund.refund_from 注释 '退款来源（1用户发起，2，云收银发起，3本地收银发起，4超时系统自动发起）'。",
                      "targetColumns": [
                        {"column": "refund_from", "table": "order_refund"}
                      ],
                      "vals": {
                        "Step2": {
                          "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.ConstraintsOfValueType",
                          "vals": {
                            "constraint": {
                              "enumVals": ["1", "2", "3", "4"],
                              "caseInsensitive": false,
                              "$id": "com.qlangtech.tis.plugin.ontology.impl.valuetype.constraints.Enum4String"
                            }
                          }
                        },
                        "Step1": {
                          "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.MetadataOfValueType",
                          "vals": {
                            "name": "RefundFrom",
                            "description": "退款来源",
                            "type": "1"
                          }
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.DefaultOntologyValueType",
                      "reason": "grid_field.field_type 注释 '字段类型'，但未列出枚举，类型为SHORT，推测为枚举。由于缺少明确枚举列表，给予medium置信度。",
                      "targetColumns": [
                        {"column": "field_type", "table": "grid_field"}
                      ],
                      "vals": {
                        "Step2": {
                          "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.ConstraintsOfValueType",
                          "vals": {
                            "constraint": {
                              "enumVals": ["1", "2", "3", "4"],
                              "caseInsensitive": false,
                              "$id": "com.qlangtech.tis.plugin.ontology.impl.valuetype.constraints.Enum4String"
                            }
                          }
                        },
                        "Step1": {
                          "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.MetadataOfValueType",
                          "vals": {
                            "name": "FieldType",
                            "description": "表格字段类型枚举",
                            "type": "1"
                          }
                        }
                      },
                      "confidence": "medium"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.DefaultOntologyValueType",
                      "reason": "order_tag.biz_from 类型为 BOOLEAN，属于典型布尔值约束。",
                      "targetColumns": [
                        {"column": "biz_from", "table": "order_tag"}
                      ],
                      "vals": {
                        "Step2": {
                          "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.ConstraintsOfValueType",
                          "vals": {
                            "constraint": {
                              "enumVals": ["true", "false"],
                              "caseInsensitive": true,
                              "$id": "com.qlangtech.tis.plugin.ontology.impl.valuetype.constraints.Enum4String"
                            }
                          }
                        },
                        "Step1": {
                          "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.MetadataOfValueType",
                          "vals": {
                            "name": "BooleanFlag",
                            "description": "布尔值标志",
                            "type": "1"
                          }
                        }
                      },
                      "confidence": "high"
                    },
                    {
                      "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.DefaultOntologyValueType",
                      "reason": "discount_detail.discount_type 类型为 INTEGER，注释 '优惠类型'，虽然无列举，但常用金额折扣相关枚举。",
                      "targetColumns": [
                        {"column": "discount_type", "table": "discount_detail"}
                      ],
                      "vals": {
                        "Step2": {
                          "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.ConstraintsOfValueType",
                          "vals": {
                            "constraint": {
                              "min": 1,
                              "max": 10,
                              "$id": "com.qlangtech.tis.plugin.ontology.impl.valuetype.constraints.Range4Integer"
                            }
                          }
                        },
                        "Step1": {
                          "impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.MetadataOfValueType",
                          "vals": {
                            "name": "DiscountType",
                            "description": "优惠类型",
                            "type": "2"
                          }
                        }
                      },
                      "confidence": "low"
                    }
                  ]
                }
                """;

        List<JSONObject> linkTypes = new ArrayList<>();
        List<JSONObject> sharedProps = new ArrayList<>();
        List<JSONObject> valueTypes = new ArrayList<>();
        List<JSONObject> glossaries = new ArrayList<>();

        StreamingJsonOntologyParser parser = new StreamingJsonOntologyParser();
        parser.setCallbacks(new StreamingJsonOntologyParser.Callbacks() {
            @Override
            public void onLinkType(JSONObject element) {
                linkTypes.add(element);
                System.out.println("LinkType parsed: " + element.toJSONString());
            }

            @Override
            public void onSharedProperty(JSONObject element) {
                sharedProps.add(element);
                System.out.println("SharedProperty parsed: " + element.toJSONString());
            }

            @Override
            public void onValueType(JSONObject element) {
                valueTypes.add(element);
                System.out.println("ValueType parsed: " + element.toJSONString());
            }

            @Override
            public void onGlossary(JSONObject element) {
                glossaries.add(element);
                System.out.println("Glossary parsed: " + element.toJSONString());
            }
        });

        List<String> chunks = Lists.newArrayList();
        int step = 20;
        for (int i = 0; i < jsonContent.length(); i += step) {
            chunks.add(StringUtils.substring(jsonContent, i, i + step));
        }

        // 模拟流式输入
        for (String chunk : chunks) {
            parser.appendChunk(chunk);
            parser.parse();
        }

        parser.finish();

        // 验证结果
        assertEquals("Should parse 2 linkTypes", 0, linkTypes.size());
//        assertEquals("order_customer", linkTypes.get(0).getString("name"));
//        assertEquals("high", linkTypes.get(0).getString("confidence"));
//        assertEquals("order_product", linkTypes.get(1).getString("name"));

        assertEquals("Should parse 1 sharedProperty", 14, sharedProps.size());
        // assertEquals("id", sharedProps.get(0).getString("name"));

        assertEquals("Should parse 1 valueType", 19, valueTypes.size());
        // assertEquals("amount", valueTypes.get(0).getString("name"));

        assertEquals("Should parse 1 glossary", 45, glossaries.size());
        // assertEquals("订单", glossaries.get(0).getString("term"));
    }

    /**
     * 测试一个更加真实的
     *
     * @throws IOException
     */
    @Test
    public void testRealStreaming() throws IOException {
        // 模拟 LLM 返回的 JSON，分块输入
        String jsonContent = """
                {
                	"ontology-glossary": [
                	  {
                		"confidence": "high",
                		"impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                		"reason": "Table name 'toy_stores' clearly represents the business entity 'store'.",
                		"vals": {
                			"description": "门店信息表",
                			"synonyms": ["门店", "商店", "店铺", "store"],
                			"target": {
                				"$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetOT",
                				"objectType": "toy_stores"
                			},
                			"term": "store"
                		}
                	},\s
                	  {
                		"confidence": "high",
                		"impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                		"reason": "Table name 'toy_products' clearly represents the business entity 'product'.",
                		"vals": {
                			"description": "玩具产品信息表",
                			"synonyms": ["产品", "玩具", "商品", "product"],
                			"target": {
                				"$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetOT",
                				"objectType": "toy_products"
                			},
                			"term": "product"
                		}
                	},\s
                	  {
                		"confidence": "high",
                		"impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                		"reason": "Table name 'toy_sales' clearly represents the business entity 'sale'.",
                		"vals": {
                			"description": "玩具销售记录表",
                			"synonyms": ["销售", "订单", "售卖", "sale"],
                			"target": {
                				"$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetOT",
                				"objectType": "toy_sales"
                			},
                			"term": "sale"
                		}
                	},
                	  {
                		"confidence": "high",
                		"impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                		"reason": "Table name 'toy_inventory' clearly represents the business entity 'inventory'.",
                		"vals": {
                			"description": "玩具库存信息表",
                			"synonyms": ["库存", "存货", "inventor", "inventory"],
                			"target": {
                				"$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetOT",
                				"objectType": "toy_inventory"
                			},
                			"term": "inventory"
                		}
                	},
                	  {
                		"confidence": "high",
                		"impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                		"reason": "Column 'Store_Name' in 'toy_stores' has clear business meaning as store identifier.",
                		"vals": {
                			"description": "门店名称",
                			"synonyms": ["门店名", "店铺名称", "store name"],
                			"target": {
                				"$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetProperty",
                				"objectType": "toy_stores",
                				"targetField": "Store_Name"
                			},
                			"term": "store_name"
                		}
                	},
                	  {
                		"confidence": "high",
                		"impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                		"reason": "Column 'Product_Name' in 'toy_products' has clear business meaning as product identifier.",
                		"vals": {
                			"description": "产品名称",
                			"synonyms": ["商品名称", "玩具名称", "product name"],
                			"target": {
                				"$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetProperty",
                				"objectType": "toy_products",
                				"targetField": "Product_Name"
                			},
                			"term": "product_name"
                		}
                	},
                	  {
                		"confidence": "high",
                		"impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                		"reason": "Column 'Stock_On_Hand' in 'toy_inventory' is a numeric stock quantity with clear business meaning.",
                		"vals": {
                			"description": "在库库存数量",
                			"synonyms": ["库存数量", "当前库存", "stock on hand"],
                			"target": {
                				"$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetProperty",
                				"objectType": "toy_inventory",
                				"targetField": "Stock_On_Hand"
                			},
                			"term": "stock_on_hand"
                		}
                	},
                	  {
                		"confidence": "high",
                		"impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                		"reason": "Column 'Units' in 'toy_sales' is a numeric sales quantity with clear business meaning.",
                		"vals": {
                			"description": "销售数量",
                			"synonyms": ["销量", "售出数量", "units sold"],
                			"target": {
                				"$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetProperty",
                				"objectType": "toy_sales",
                				"targetField": "Units"
                			},
                			"term": "units"
                		}
                	},
                	  {
                		"confidence": "high",
                		"impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                		"reason": "Column 'Product_Price' in 'toy_products' is a monetary value with clear business meaning.",
                		"vals": {
                			"description": "产品售价",
                			"synonyms": ["价格", "单价", "product price"],
                			"target": {
                				"$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetProperty",
                				"objectType": "toy_products",
                				"targetField": "Product_Price"
                			},
                			"term": "product_price"
                		}
                	},
                	  {
                		"confidence": "high",
                		"impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                		"reason": "Column 'Product_Cost' in 'toy_products' is a monetary value with clear business meaning.",
                		"vals": {
                			"description": "产品成本",
                			"synonyms": ["成本价", "进货价", "product cost"],
                			"target": {
                				"$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetProperty",
                				"objectType": "toy_products",
                				"targetField": "Product_Cost"
                			},
                			"term": "product_cost"
                		}
                	},
                	  {
                		"confidence": "high",
                		"impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                		"reason": "Column 'Store_City' in 'toy_stores' is a location attribute with clear business meaning.",
                		"vals": {
                			"description": "门店所在城市",
                			"synonyms": ["城市", "店址城市", "store city"],
                			"target": {
                				"$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetProperty",
                				"objectType": "toy_stores",
                				"targetField": "Store_City"
                			},
                			"term": "store_city"
                		}
                	},
                	  {
                		"confidence": "high",
                		"impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                		"reason": "Column 'Date' in 'toy_sales' is a temporal attribute with clear business meaning.",
                		"vals": {
                			"description": "销售日期",
                			"synonyms": ["日期", "销售时间", "sale date"],
                			"target": {
                				"$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetProperty",
                				"objectType": "toy_sales",
                				"targetField": "Date"
                			},
                			"term": "date"
                		}
                	},
                	  {
                		"confidence": "medium",
                		"impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                		"reason": "'Total Sales Amount' is a common business metric derived from SUM of Units * Product_Price, though not directly present; inferred from sales + product context.",
                		"vals": {
                			"description": "总销售额",
                			"synonyms": ["销售总额", "GMV", "total sales amount"],
                			"target": {
                				"$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetMetricExpr",
                				"sql": "SUM(toy_sales.Units * CAST(toy_products.Product_Price AS LONG)) FROM toy_sales JOIN toy_products ON toy_sales.Product_ID = toy_products.Product_ID"
                			},
                			"term": "total_sales_amount"
                		}
                	},
                	  {
                		"confidence": "medium",
                		"impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                		"reason": "'Inventory Value' is a common business metric derived from SUM of Stock_On_Hand * Product_Price, though not directly present; inferred from inventory + product context.",
                		"vals": {
                			"description": "库存总价值",
                			"synonyms": ["库存金额", "inventory value"],
                			"target": {
                				"$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetMetricExpr",
                				"sql": "SUM(toy_inventory.Stock_On_Hand * CAST(toy_products.Product_Price AS LONG)) FROM toy_inventory JOIN toy_products ON toy_inventory.Product_ID = toy_products.Product_ID"
                			},
                			"term": "inventory_value"
                		}
                	},
                	  {
                		"confidence": "medium",
                		"impl": "com.qlangtech.tis.plugin.ontology.impl.glossary.DefaultOntologyGlossary",
                		"reason": "'Active Stores Count' is a common business metric derived from COUNT of stores, though not directly present; inferred from store context.",
                		"vals": {
                			"description": "门店总数",
                			"synonyms": ["门店数量", "active stores count"],
                			"target": {
                				"$id": "com.qlangtech.tis.plugin.ontology.impl.glossary.GlossaryTargetMetricExpr",
                				"sql": "COUNT(*) FROM toy_stores"
                			},
                			"term": "active_stores_count"
                		}
                	}],
                	"link-types": [
                	  {
                		"confidence": "high",
                		"impl": "com.qlangtech.tis.plugin.ontology.impl.linker.DefaultOntologyLinker",
                		"reason": "toy_inventory has Store_ID and Product_ID as foreign keys, and also contains Stock_On_Hand (non-ID column) → Backing object type (token=3).",
                		"vals": {
                			"Step1": {
                				"impl": "com.qlangtech.tis.plugin.ontology.impl.linker.RelationshipTypeSetter",
                				"vals": {
                					"relationshipType": 3
                				}
                			},
                			"Step2": {
                				"impl": "com.qlangtech.tis.plugin.ontology.impl.linker.RelationshipTypeBackingObjectType",
                				"vals": {
                					"joinObjectType": {
                						"$id": "com.qlangtech.tis.plugin.ontology.impl.linker.JoinReference",
                						"objectType": "toy_inventory",
                						"rightTargetField": "Product_ID",
                						"targetField": "Store_ID"
                					},
                					"leftObjectType": "toy_stores",
                					"rightObjectType": "toy_products"
                				}
                			}
                		}
                	},\s
                	  {
                		"confidence": "high",
                		"impl": "com.qlangtech.tis.plugin.ontology.impl.linker.DefaultOntologyLinker",
                		"reason": "toy_sales has Store_ID and Product_ID as foreign keys, and also contains Units and Date (non-ID columns) → Backing object type (token=3).",
                		"vals": {
                			"Step1": {
                				"impl": "com.qlangtech.tis.plugin.ontology.impl.linker.RelationshipTypeSetter",
                				"vals": {
                					"relationshipType": 3
                				}
                			},
                			"Step2": {
                				"impl": "com.qlangtech.tis.plugin.ontology.impl.linker.RelationshipTypeBackingObjectType",
                				"vals": {
                					"joinObjectType": {
                						"$id": "com.qlangtech.tis.plugin.ontology.impl.linker.JoinReference",
                						"objectType": "toy_sales",
                						"rightTargetField": "Product_ID",
                						"targetField": "Store_ID"
                					},
                					"leftObjectType": "toy_stores",
                					"rightObjectType": "toy_products"
                				}
                			}
                		}
                	},
                	  {
                		"confidence": "medium",
                		"impl": "com.qlangtech.tis.plugin.ontology.impl.linker.DefaultOntologyLinker",
                		"reason": "toy_stores has Store_ID as PK, and toy_inventory & toy_sales reference it via Store_ID → Object type foreign key (token=1).",
                		"vals": {
                			"Step1": {
                				"impl": "com.qlangtech.tis.plugin.ontology.impl.linker.RelationshipTypeSetter",
                				"vals": {
                					"relationshipType": 1
                				}
                			},
                			"Step2": {
                				"impl": "com.qlangtech.tis.plugin.ontology.impl.linker.RelationshipTypeObjectTypeForeignKeys",
                				"vals": {
                					"left": {
                						"$id": "com.qlangtech.tis.plugin.ontology.impl.linker.LinkReference",
                						"objectType": "toy_stores",
                						"targetField": "Store_ID"
                					},
                					"right": {
                						"$id": "com.qlangtech.tis.plugin.ontology.impl.linker.LinkReference",
                						"objectType": "toy_inventory",
                						"targetField": "Store_ID"
                					}
                				}
                			}
                		}
                
                
                	},\s
                	  {
                		"confidence": "medium",
                		"impl": "com.qlangtech.tis.plugin.ontology.impl.linker.DefaultOntologyLinker",
                		"reason": "toy_stores has Store_ID as PK, and toy_sales references it via Store_ID → Object type foreign key (token=1).",
                		"vals": {
                			"Step1": {
                				"impl": "com.qlangtech.tis.plugin.ontology.impl.linker.RelationshipTypeSetter",
                				"vals": {
                					"relationshipType": 1
                				}
                			},
                			"Step2": {
                				"impl": "com.qlangtech.tis.plugin.ontology.impl.linker.RelationshipTypeObjectTypeForeignKeys",
                				"vals": {
                					"left": {
                						"$id": "com.qlangtech.tis.plugin.ontology.impl.linker.LinkReference",
                						"objectType": "toy_stores",
                						"targetField": "Store_ID"
                					},
                					"right": {
                						"$id": "com.qlangtech.tis.plugin.ontology.impl.linker.LinkReference",
                						"objectType": "toy_sales",
                						"targetField": "Store_ID"
                					}
                				}
                			}
                		}
                
                
                	},
                	  {
                		"confidence": "medium",
                		"impl": "com.qlangtech.tis.plugin.ontology.impl.linker.DefaultOntologyLinker",
                		"reason": "toy_products has Product_ID as PK, and toy_inventory references it via Product_ID → Object type foreign key (token=1).",
                		"vals": {
                			"Step1": {
                				"impl": "com.qlangtech.tis.plugin.ontology.impl.linker.RelationshipTypeSetter",
                				"vals": {
                					"relationshipType": 1
                				}
                			},
                			"Step2": {
                				"impl": "com.qlangtech.tis.plugin.ontology.impl.linker.RelationshipTypeObjectTypeForeignKeys",
                				"vals": {
                					"left": {
                						"$id": "com.qlangtech.tis.plugin.ontology.impl.linker.LinkReference",
                						"objectType": "toy_products",
                						"targetField": "Product_ID"
                					},
                					"right": {
                						"$id": "com.qlangtech.tis.plugin.ontology.impl.linker.LinkReference",
                						"objectType": "toy_inventory",
                						"targetField": "Product_ID"
                					}
                				}
                			}
                		}
                
                
                	},
                	  {
                		"confidence": "medium",
                		"impl": "com.qlangtech.tis.plugin.ontology.impl.linker.DefaultOntologyLinker",
                		"reason": "toy_products has Product_ID as PK, and toy_sales references it via Product_ID → Object type foreign key (token=1).",
                		"vals": {
                			"Step1": {
                				"impl": "com.qlangtech.tis.plugin.ontology.impl.linker.RelationshipTypeSetter",
                				"vals": {
                					"relationshipType": 1
                				}
                			},
                			"Step2": {
                				"impl": "com.qlangtech.tis.plugin.ontology.impl.linker.RelationshipTypeObjectTypeForeignKeys",
                				"vals": {
                					"left": {
                						"$id": "com.qlangtech.tis.plugin.ontology.impl.linker.LinkReference",
                						"objectType": "toy_products",
                						"targetField": "Product_ID"
                					},
                					"right": {
                						"$id": "com.qlangtech.tis.plugin.ontology.impl.linker.LinkReference",
                						"objectType": "toy_sales",
                						"targetField": "Product_ID"
                					}
                				}
                			}
                		}
                
                
                	}],
                	"ontology-shared-property": [
                	  {
                		"confidence": "high",
                		"impl": "com.qlangtech.tis.plugin.ontology.impl.sharedproperty.DefaultOntologySharedProperty",
                		"reason": "Store_ID appears in toy_stores (PK), toy_inventory, and toy_sales → shared identity property.",
                		"targetColumns": [{
                			"column": "Store_ID",
                			"table": "toy_stores"
                		}, {
                			"column": "Store_ID",
                			"table": "toy_inventory"
                		}, {
                			"column": "Store_ID",
                			"table": "toy_sales"
                		}],
                		"vals": {
                			"description": "门店唯一标识符",
                			"name": "store_id",
                			"type": "1"
                		}
                	}],
                	"ontology-value-type": [
                	  {
                		"confidence": "high",
                		"impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.DefaultOntologyValueType",
                		"reason": "Store_ID is STRING and used as primary/foreign key across multiple tables; likely follows a consistent format like 'S001', 'STORE-101' → Range constraint implied by length/format.",
                		"targetColumns": [{
                			"column": "Store_ID",
                			"table": "toy_stores"
                		}, {
                			"column": "Store_ID",
                			"table": "toy_inventory"
                		}, {
                			"column": "Store_ID",
                			"table": "toy_sales"
                		}],
                		"vals": {
                			"Step1": {
                				"impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.MetadataOfValueType",
                				"vals": {
                					"description": "门店编码，格式如 S001 或 STORE-101",
                					"name": "StoreCode",
                					"type": 1
                				}
                			},
                			"Step2": {
                				"impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.ConstraintsOfValueType",
                				"vals": {
                					"constraint": {
                						"$id": "com.qlangtech.tis.plugin.ontology.impl.valuetype.constraints.Range4String",
                						"max": 10,
                						"min": 3
                					}
                				}
                			}
                		}
                	},\s
                	  {
                		"confidence": "high",
                		"impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.DefaultOntologyValueType",
                		"reason": "Product_ID is STRING and used as primary/foreign key across multiple tables; likely follows a consistent format like 'P123', 'TOY-456' → Range constraint implied by length/format.",
                		"targetColumns": [{
                			"column": "Product_ID",
                			"table": "toy_products"
                		}, {
                			"column": "Product_ID",
                			"table": "toy_inventory"
                		}, {
                			"column": "Product_ID",
                			"table": "toy_sales"
                		}],
                		"vals": {
                			"Step1": {
                				"impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.MetadataOfValueType",
                				"vals": {
                					"description": "产品编码，格式如 P123 或 TOY-456",
                					"name": "ProductCode",
                					"type": 1
                				}
                			},
                			"Step2": {
                				"impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.ConstraintsOfValueType",
                				"vals": {
                					"constraint": {
                						"$id": "com.qlangtech.tis.plugin.ontology.impl.valuetype.constraints.Range4String",
                						"max": 10,
                						"min": 3
                					}
                				}
                			}
                		}
                	},
                	  {
                		"confidence": "high",
                		"impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.DefaultOntologyValueType",
                		"reason": "Stock_On_Hand and Units are both LONG and represent non-negative quantities → Range constraint (>= 0).",
                		"targetColumns": [{
                			"column": "Stock_On_Hand",
                			"table": "toy_inventory"
                		}, {
                			"column": "Units",
                			"table": "toy_sales"
                		}],
                		"vals": {
                			"Step1": {
                				"impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.MetadataOfValueType",
                				"vals": {
                					"description": "非负整数量值，表示库存或销售单位数",
                					"name": "NonNegativeQuantity",
                					"type": 1
                				}
                			},
                			"Step2": {
                				"impl": "com.qlangtech.tis.plugin.ontology.impl.valuetype.ConstraintsOfValueType",
                				"vals": {
                					"constraint": {
                						"$id": "com.qlangtech.tis.plugin.ontology.impl.valuetype.constraints.Range4String",
                						"max": 9223372036854775807,
                						"min": 0
                					}
                				}
                			}
                		}
                	}]
                }
                """;

        List<JSONObject> linkTypes = new ArrayList<>();
        List<JSONObject> sharedProps = new ArrayList<>();
        List<JSONObject> valueTypes = new ArrayList<>();
        List<JSONObject> glossaries = new ArrayList<>();

        StreamingJsonOntologyParser parser = new StreamingJsonOntologyParser();
        parser.setCallbacks(new StreamingJsonOntologyParser.Callbacks() {
            @Override
            public void onLinkType(JSONObject element) {
                linkTypes.add(element);
                System.out.println("LinkType parsed: " + element.toJSONString());
            }

            @Override
            public void onSharedProperty(JSONObject element) {
                sharedProps.add(element);
                System.out.println("SharedProperty parsed: " + element.toJSONString());
            }

            @Override
            public void onValueType(JSONObject element) {
                valueTypes.add(element);
                System.out.println("ValueType parsed: " + element.toJSONString());
            }

            @Override
            public void onGlossary(JSONObject element) {
                glossaries.add(element);
                System.out.println("Glossary parsed: " + element.toJSONString());
            }
        });

        List<String> chunks = Lists.newArrayList();
        int step = 20;
        for (int i = 0; i < jsonContent.length(); i += step) {
            chunks.add(StringUtils.substring(jsonContent, i, i + step));
        }

        // 模拟流式输入
        for (String chunk : chunks) {
            parser.appendChunk(chunk);
            parser.parse();
        }

        parser.finish();

        // 验证结果
        assertEquals("Should parse 2 linkTypes", 6, linkTypes.size());
//        assertEquals("order_customer", linkTypes.get(0).getString("name"));
//        assertEquals("high", linkTypes.get(0).getString("confidence"));
//        assertEquals("order_product", linkTypes.get(1).getString("name"));

        assertEquals("Should parse 1 sharedProperty", 1, sharedProps.size());
        // assertEquals("id", sharedProps.get(0).getString("name"));

        assertEquals("Should parse 1 valueType", 3, valueTypes.size());
        // assertEquals("amount", valueTypes.get(0).getString("name"));

        assertEquals("Should parse 1 glossary", 15, glossaries.size());
        // assertEquals("订单", glossaries.get(0).getString("term"));
    }


    @Test
    public void testBasicStreaming() throws IOException {
        // 模拟 LLM 返回的 JSON，分块输入
        String[] chunks = {
                "{\"link-types\":[",
                "{\"name\":\"order_customer\",",
                "\"confidence\":\"high\"}",
                ",{\"name\":\"order_product\",",
                "\"confidence\":\"medium\"}],",
                "\"ontology-shared-property\":[",
                "{\"name\":\"id\",\"type\":\"string\"}",
                "],\"ontology-value-type\":[",
                "{\"name\":\"amount\",\"type\":\"decimal\"}",
                "],\"ontology-glossary\":[",
                "{\"term\":\"订单\",\"definition\":\"客户购买记录\"}",
                "]}"
        };

        List<JSONObject> linkTypes = new ArrayList<>();
        List<JSONObject> sharedProps = new ArrayList<>();
        List<JSONObject> valueTypes = new ArrayList<>();
        List<JSONObject> glossaries = new ArrayList<>();

        StreamingJsonOntologyParser parser = new StreamingJsonOntologyParser();
        parser.setCallbacks(new StreamingJsonOntologyParser.Callbacks() {
            @Override
            public void onLinkType(JSONObject element) {
                linkTypes.add(element);
                System.out.println("LinkType parsed: " + element.toJSONString());
            }

            @Override
            public void onSharedProperty(JSONObject element) {
                sharedProps.add(element);
                System.out.println("SharedProperty parsed: " + element.toJSONString());
            }

            @Override
            public void onValueType(JSONObject element) {
                valueTypes.add(element);
                System.out.println("ValueType parsed: " + element.toJSONString());
            }

            @Override
            public void onGlossary(JSONObject element) {
                glossaries.add(element);
                System.out.println("Glossary parsed: " + element.toJSONString());
            }
        });

        // 模拟流式输入
        for (String chunk : chunks) {
            parser.appendChunk(chunk);
            parser.parse();
        }

        parser.finish();

        // 验证结果
        assertEquals("Should parse 2 linkTypes", 2, linkTypes.size());
        assertEquals("order_customer", linkTypes.get(0).getString("name"));
        assertEquals("high", linkTypes.get(0).getString("confidence"));
        assertEquals("order_product", linkTypes.get(1).getString("name"));

        assertEquals("Should parse 1 sharedProperty", 1, sharedProps.size());
        assertEquals("id", sharedProps.get(0).getString("name"));

        assertEquals("Should parse 1 valueType", 1, valueTypes.size());
        assertEquals("amount", valueTypes.get(0).getString("name"));

        assertEquals("Should parse 1 glossary", 1, glossaries.size());
        assertEquals("订单", glossaries.get(0).getString("term"));
    }

    @Test
    public void testChunkingInMiddleOfString() throws IOException {
        // 测试在字符串中间分块的情况
        String[] chunks = {
                "{\"link-types\":[{\"name\":\"order",
                "_customer\",\"description\":\"Links ord",
                "er to customer\"}]}"
        };

        List<JSONObject> linkTypes = new ArrayList<>();

        StreamingJsonOntologyParser parser = new StreamingJsonOntologyParser();
        parser.setCallbacks(new StreamingJsonOntologyParser.Callbacks() {
            @Override
            public void onLinkType(JSONObject element) {
                linkTypes.add(element);
            }

            @Override
            public void onSharedProperty(JSONObject element) {
            }

            @Override
            public void onValueType(JSONObject element) {
            }

            @Override
            public void onGlossary(JSONObject element) {
            }
        });

        for (String chunk : chunks) {
            parser.appendChunk(chunk);
            parser.parse();
        }

        parser.finish();

        assertEquals("Should parse 1 linkType", 1, linkTypes.size());
        assertEquals("order_customer", linkTypes.get(0).getString("name"));
        assertEquals("Links order to customer", linkTypes.get(0).getString("description"));
    }

    @Test
    public void testEmptyArrays() throws IOException {
        String json = "{\"link-types\":[],\"ontology-shared-property\":[],\"ontology-value-type\":[],\"ontology-glossary\":[]}";

        List<JSONObject> allElements = new ArrayList<>();

        StreamingJsonOntologyParser parser = new StreamingJsonOntologyParser();
        parser.setCallbacks(new StreamingJsonOntologyParser.Callbacks() {
            @Override
            public void onLinkType(JSONObject element) {
                allElements.add(element);
            }

            @Override
            public void onSharedProperty(JSONObject element) {
                allElements.add(element);
            }

            @Override
            public void onValueType(JSONObject element) {
                allElements.add(element);
            }

            @Override
            public void onGlossary(JSONObject element) {
                allElements.add(element);
            }
        });

        parser.appendChunk(json);
        parser.parse();
        parser.finish();

        assertEquals("Should parse 0 elements from empty arrays", 0, allElements.size());
    }

    @Test
    public void testNestedObjects() throws IOException {
        // 测试嵌套对象
        String json = "{\"link-types\":[{\"name\":\"order_detail\",\"metadata\":{\"source\":\"database\",\"version\":1}}]}";

        List<JSONObject> linkTypes = new ArrayList<>();

        StreamingJsonOntologyParser parser = new StreamingJsonOntologyParser();
        parser.setCallbacks(new StreamingJsonOntologyParser.Callbacks() {
            @Override
            public void onLinkType(JSONObject element) {
                linkTypes.add(element);
            }

            @Override
            public void onSharedProperty(JSONObject element) {
            }

            @Override
            public void onValueType(JSONObject element) {
            }

            @Override
            public void onGlossary(JSONObject element) {
            }
        });

        parser.appendChunk(json);
        parser.parse();
        parser.finish();

        assertEquals("Should parse 1 linkType with nested object", 1, linkTypes.size());
        assertEquals("order_detail", linkTypes.get(0).getString("name"));
        assertNotNull(linkTypes.get(0).getJSONObject("metadata"));
        assertEquals("database", linkTypes.get(0).getJSONObject("metadata").getString("source"));
    }
}
