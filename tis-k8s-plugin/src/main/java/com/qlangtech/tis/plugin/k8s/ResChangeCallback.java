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

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-12-28 17:58
 **/
public interface ResChangeCallback {
    public void apply(K8SResChangeReason changeReason, String podName);


    /**
     * 是否终止K8S 资源变化监听
     *
     * @param podFaildCount        pod 执行失败数量
     * @param podCompleteCount     pod 执行完成（包括失败）数量
     * @param rcCompleteCount      RC 执行完成数量
     * @param expectResChangeCount 期望达到的资源变化数量
     * @return
     */
    public default boolean isBreakEventWatch(int podFaildCount, int podCompleteCount, int rcCompleteCount, final int expectResChangeCount) {
        return (podCompleteCount >= expectResChangeCount);
    }
}
