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

package com.qlangtech.tis.plugin.k8s;

import com.qlangtech.tis.plugin.k8s.K8SUtils.K8SResChangeReason;
import com.qlangtech.tis.plugin.k8s.K8SUtils.PodStat;
import io.kubernetes.client.openapi.models.V1Pod;

import java.util.Map;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-12-28 17:58
 **/
public interface ResChangeCallback {
    /**
     * 在Waiting等待过程中是否要获取已有的Pods，在scala pods避免要出现刚添加的pod，随即马上去掉，此时程序识别成又添加了一个pod的情况
     *
     * @return
     */
    public default boolean shallGetExistPods() {
        return false;
    }

    public default void apply(K8SResChangeReason changeReason, String podName) {

    }


    /**
     *
     *
     * @param         pod 执行失败数量
     * @param podCompleteCount     pod 执行完成（包括失败）数量
     * @param rcCompleteCount      RC 执行完成数量
     * @param expectResChangeCount 期望达到的资源变化数量
     * @return
     */
    /**
     * 是否终止K8S 资源变化监听
     *
     * @param relevantPodNames
     * @param expectResChangeCount
     * @return
     */
    public default boolean isBreakEventWatch(final Map<String, PodStat> relevantPodNames, final int expectResChangeCount) {
        return (relevantPodNames.values().size() >= expectResChangeCount);
    }

    default void applyDefaultPodPhase(final Map<String, PodStat> relevantPodNames, V1Pod pod) {

    }
}
