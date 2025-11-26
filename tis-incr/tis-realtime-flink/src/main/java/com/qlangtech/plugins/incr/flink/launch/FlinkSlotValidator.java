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

package com.qlangtech.plugins.incr.flink.launch;

import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.lang.TisException;
import org.apache.flink.client.program.rest.RestClusterClient;
import org.apache.flink.runtime.rest.handler.legacy.messages.ClusterOverviewWithVersion;
import org.apache.flink.runtime.rest.messages.EmptyMessageParameters;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagerInfo;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagersHeaders;
import org.apache.flink.runtime.rest.messages.taskmanager.TaskManagersInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

/**
 * Flink集群Slot校验工具类
 * 用于在部署Flink Job之前校验集群是否有足够的可用slot
 *
 * @author: 百岁(baisui@qlangtech.com)
 * @create: 2025-11-26
 **/
public class FlinkSlotValidator {
    private static final Logger logger = LoggerFactory.getLogger(FlinkSlotValidator.class);

    /**
     * 校验Flink集群是否有足够的可用slot
     *
     * @param restClient RestClusterClient实例
     * @param requiredSlots 所需的slot数量(等于配置的parallelism)
     * @param collection 任务目标资源名称
     * @throws TisException 当可用slot不足时抛出异常
     */
    public static void validateAvailableSlots(
            RestClusterClient<?> restClient,
            int requiredSlots,
            TargetResName collection) throws TisException {

        if (requiredSlots <= 0) {
            throw new IllegalArgumentException("Required slots must be greater than 0, but got: " + requiredSlots);
        }

        String clusterUrl = restClient.getWebInterfaceURL();

        try {
            logger.info("Starting slot validation for job: {}, required slots: {}, cluster: {}",
                    collection.getName(), requiredSlots, clusterUrl);

            // 使用Flink原生API获取集群概览信息
            CompletableFuture<ClusterOverviewWithVersion> overviewFuture = restClient.sendRequest(
                    org.apache.flink.runtime.rest.messages.ClusterOverviewHeaders.getInstance(),
                    EmptyMessageParameters.getInstance(),
                    EmptyRequestBody.getInstance()
            );
            ClusterOverviewWithVersion overview = overviewFuture.get(10, TimeUnit.SECONDS);

            // 使用Flink原生API获取TaskManager详细信息
            CompletableFuture<TaskManagersInfo> taskManagersFuture = restClient.sendRequest(
                    TaskManagersHeaders.getInstance(),
                    EmptyMessageParameters.getInstance(),
                    EmptyRequestBody.getInstance()
            );
            TaskManagersInfo taskManagersInfo = taskManagersFuture.get(10, TimeUnit.SECONDS);

            // 计算可用slot总数
            int totalSlots = overview.getNumSlotsTotal();
            int availableSlots = overview.getNumSlotsAvailable();
            int occupiedSlots = totalSlots - availableSlots;
            int taskManagerCount = overview.getNumTaskManagersConnected();

            logger.info("Cluster slot status - Total: {}, Available: {}, Occupied: {}, TaskManagers: {}",
                    totalSlots, availableSlots, occupiedSlots, taskManagerCount);

            // 校验可用slot是否足够
            if (availableSlots < requiredSlots) {
                String errorMessage = buildInsufficientSlotsErrorMessage(
                        collection,
                        requiredSlots,
                        availableSlots,
                        totalSlots,
                        occupiedSlots,
                        taskManagerCount,
                        taskManagersInfo,
                        clusterUrl
                );

               // logger.error(errorMessage);
                throw TisException.create(errorMessage);
            }

            logger.info("Slot validation passed. Available slots ({}) >= Required slots ({})",
                    availableSlots, requiredSlots);

        } catch (TisException e) {
            throw e;
        } catch (Exception e) {
            String errorMsg = "Failed to validate available slots for job: " + collection.getName()
                    + ", cluster address: " + clusterUrl;
            logger.error(errorMsg, e);
            throw TisException.create(errorMsg, e);
        }
    }

    /**
     * 构建slot不足时的详细错误信息
     */
    private static String buildInsufficientSlotsErrorMessage(
            TargetResName collection,
            int requiredSlots,
            int availableSlots,
            int totalSlots,
            int occupiedSlots,
            int taskManagerCount,
            TaskManagersInfo taskManagersInfo,
            String clusterUrl) {

        StringBuilder errorMsg = new StringBuilder();
        errorMsg.append("Insufficient available slots in Flink cluster for job: ").append(collection.getName()).append("\n");
        errorMsg.append("========================================\n");
        errorMsg.append("Required slots: ").append(requiredSlots).append("\n");
        errorMsg.append("Available slots: ").append(availableSlots).append("\n");
        errorMsg.append("Total slots: ").append(totalSlots).append("\n");
        errorMsg.append("Occupied slots: ").append(occupiedSlots).append("\n");
        errorMsg.append("TaskManager count: ").append(taskManagerCount).append("\n");

        // 添加TaskManager详细信息
        if (taskManagersInfo != null && taskManagersInfo.getTaskManagerInfos() != null) {
            Collection<TaskManagerInfo> taskManagers = taskManagersInfo.getTaskManagerInfos();
            if (!taskManagers.isEmpty()) {
                errorMsg.append("\nTaskManager Details:\n");
                int index = 1;
                for (TaskManagerInfo tmInfo : taskManagers) {
                    errorMsg.append("  [").append(index++).append("] ");
                    errorMsg.append("ID: ").append(tmInfo.getResourceId().getResourceIdString());
                    errorMsg.append(", Slots: ").append(tmInfo.getNumberSlots());
                    errorMsg.append(", Free: ").append(tmInfo.getNumberAvailableSlots());
                    errorMsg.append("\n");
                }
            }
        }

        errorMsg.append("\nCluster Address: ").append(clusterUrl).append("\n");
        errorMsg.append("========================================\n");
        errorMsg.append("Suggestions:\n");
        errorMsg.append("  1. Increase the number of TaskManager instances\n");
        errorMsg.append("  2. Increase slots per TaskManager (taskmanager.numberOfTaskSlots)\n");
        errorMsg.append("  3. Reduce the parallelism from ").append(requiredSlots)
                .append(" to ").append(availableSlots).append(" or less\n");
        errorMsg.append("  4. Wait for running jobs to complete and release slots\n");

        return errorMsg.toString();
    }
}
