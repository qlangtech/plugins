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
                	"glossaries": [
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
                	"linkTypes": [
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
                	"sharedProperties": [
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
                	"valueTypes": [
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
                "{\"linkTypes\":[",
                "{\"name\":\"order_customer\",",
                "\"confidence\":\"high\"}",
                ",{\"name\":\"order_product\",",
                "\"confidence\":\"medium\"}],",
                "\"sharedProperties\":[",
                "{\"name\":\"id\",\"type\":\"string\"}",
                "],\"valueTypes\":[",
                "{\"name\":\"amount\",\"type\":\"decimal\"}",
                "],\"glossaries\":[",
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
                "{\"linkTypes\":[{\"name\":\"order",
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
        String json = "{\"linkTypes\":[],\"sharedProperties\":[],\"valueTypes\":[],\"glossaries\":[]}";

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
        String json = "{\"linkTypes\":[{\"name\":\"order_detail\",\"metadata\":{\"source\":\"database\",\"version\":1}}]}";

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
