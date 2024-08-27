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

package com.qlangtech.tis.plugin.datax.doplinscheduler;

import com.qlangtech.tis.plugin.datax.IWorkflowNode;
import com.qlangtech.tis.sql.parser.meta.NodeType;

import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-08-21 16:30
 **/
public class DSWorkflowNode implements IWorkflowNode {
    private final String nodeName;
    private final String nodeParams;
    private final Long jobId;
    private final Long nodeId;
    private final NodeType nodeType;

    public DSWorkflowNode(String nodeName
            , NodeType nodeType, String nodeParams, Long jobId, Long nodeId) {
        this.nodeName = nodeName;
        this.nodeParams = nodeParams;
        this.jobId = jobId;
        this.nodeId = nodeId;
        this.nodeType = Objects.requireNonNull(nodeType, "nodeType can not be null");
    }

    @Override
    public String getNodeName() {
        return this.nodeName;
    }

    @Override
    public String getNodeParams() {
        return this.nodeParams;
    }

    @Override
    public NodeType getNodeType() {
        return this.nodeType;
    }

    @Override
    public boolean getEnable() {
        return true;
    }

    @Override
    public Long getJobId() {
        return this.jobId;
    }

    @Override
    public Long getNodeId() {
        return this.nodeId;
    }
}
