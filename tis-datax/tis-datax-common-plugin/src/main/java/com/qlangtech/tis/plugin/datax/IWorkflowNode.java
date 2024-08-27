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

package com.qlangtech.tis.plugin.datax;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.sql.parser.ISqlTask;
import com.qlangtech.tis.sql.parser.meta.NodeType;

import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-08-16 11:23
 **/
public interface IWorkflowNode {
    String getNodeName();

    String getNodeParams();

    NodeType getNodeType();

    default Optional<ISqlTask.SqlTaskCfg> parseSqlTaskCfg() throws NodeType.NodeTypeParseException {
        JSONObject nodeCfg = JSON.parseObject(this.getNodeParams());
        if (nodeCfg.containsKey(ISqlTask.KEY_EXECUTE_TYPE)) {
            return Optional.of(ISqlTask.toCfg(nodeCfg));
        } else {
            return Optional.empty();
        }
    }

    boolean getEnable();

    Long getJobId();

    Long getNodeId();

//    public enum Type {
//        /**
//         * ETL处理器的开始节点
//         */
//        START("start")
//        /**
//         * ETL的数据抽取节点（E）
//         */
//        , DUMP("dump")
//        /**
//         *ETL的数据处理节点（T）
//         */
//        , JOIN("join");
//
//        private final String token;
//
//        private Type(String token) {
//            this.token = token;
//        }
//
//        public static Type parse(String token) {
//            for (Type t : Type.values()) {
//                if (t.token.equalsIgnoreCase(token)) {
//                    return t;
//                }
//            }
//            throw new IllegalStateException("illegal token :" + token);
//        }
//    }
}
