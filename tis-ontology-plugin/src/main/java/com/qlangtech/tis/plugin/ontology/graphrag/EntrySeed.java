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
package com.qlangtech.tis.plugin.ontology.graphrag;

/**
 * Step1 召回得到的入口节点（含权重）。
 *
 * @param kind   命中节点种类：ObjectType / Property / SharedProperty / Glossary
 * @param otName 当 kind=Property 时绑定的 OT 名；其它种类为空
 * @param name   节点主键（OT/SP/Property 名）或 Glossary.term
 * @param score  归一化打分（0~1，越大越相关），同源融合后取 max
 * @param source 命中来源（dict / vec_ot / vec_prop / vec_sp / vec_gloss / keyword）
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/1
 */
record EntrySeed(SeedKind kind, String otName, String name, double score, String source) {

    enum SeedKind { ObjectType, Property, SharedProperty, Glossary }

    String key() {
        return kind.name() + "|" + (otName == null ? "" : otName) + "|" + name;
    }
}
