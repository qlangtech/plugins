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

package com.qlangtech.tis.plugin.datax.doplinscheduler.export;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.lang.TisException;
import com.qlangtech.tis.manage.common.AppAndRuntime;
import com.qlangtech.tis.manage.common.HttpUtils.PostParam;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.doplinscheduler.export.DolphinSchedulerURLBuilder.DolphinSchedulerResponse;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * 对应dolphinScheduler中任务组概念，用以来控制工作流中的job并发数目，可以有效防止由于大量同步任务并发执行导致业务数据库过载
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-09-06 16:41
 **/
public class DSTaskGroup implements Describable<DSTaskGroup> {
    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity, Validator.forbid_start_with_number})
    public String groupName;

    @FormField(ordinal = 1, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public int parallelism;


    public static String dftGroupName() {
        AppAndRuntime appAndRuntime = Objects.requireNonNull(AppAndRuntime.getAppAndRuntime(), "appAndRuntime can not be null");
        return appAndRuntime.getAppName() + "-task-group";
    }


    /**
     * http://192.168.28.201:12345/dolphinscheduler/swagger-ui/index.html?language=zh_CN&lang=cn#/task%20group%20related%20operation/createTaskGroup
     *
     * @param exportTISPipelineToDolphinscheduler
     */
    public TaskGroup addTaskGroup(ExportTISPipelineToDolphinscheduler exportTISPipelineToDolphinscheduler) {

        DolphinSchedulerEndpoint endpoint = exportTISPipelineToDolphinscheduler.getDSEndpoint();
        // 先检查是否存在
        // http://192.168.28.201:12345/dolphinscheduler/swagger-ui/index.html?language=zh_CN&lang=cn#/task%20group%20related%20operation/queryTaskGroupByCode
        DolphinSchedulerResponse response = endpoint.createSchedulerURLBuilder()
                .appendSubPath("task-group", "query-list-by-projectCode")
                .appendPageParam()
//                .appendQueryParam(DolphinSchedulerURLBuilder.FIELD_PAGE_NO, 1)
//                .appendQueryParam(DolphinSchedulerURLBuilder.FIELD_PAGE_SIZE, 100)
                .appendQueryParam(ExportTISPipelineToDolphinscheduler.FIELD_PROJECT_CODE, exportTISPipelineToDolphinscheduler.projectCode)
                .applyGet();
        if (!response.isSuccess()) {
            throw new IllegalStateException(response.errorDescribe());
        }
/**
 * <pre>
 * {
 * 	"totalList": [{
 * 		"useSize": 0,
 * 		"projectCode": 117442916207136,
 * 		"createTime": "2024-09-06 15:53:32",
 * 		"name": "test-tis-datax-job-group",
 * 		"description": "",
 * 		"groupSize": 1,
 * 		"updateTime": "2024-09-06 16:04:29",
 * 		"id": 1,
 * 		"userId": 1,
 * 		"status": "YES"
 *        }],
 * 	"total": 1,
 * 	"totalPage": 1,
 * 	"pageNo": 0,
 * 	"pageSize": 100,
 * 	"currentPage": 1
 * }
 * </pre>
 */
        Map<String, TaskGroup> existTaskGroups = Maps.newHashMap();
        TaskGroup tskGroup = null;
        JSONObject data = response.getData();
        JSONArray totalList = data.getJSONArray("totalList");
        //  String taskGroupName = null;

        for (Object o : totalList) {
            JSONObject taskGroup = (JSONObject) o;
            //    taskGroupName = taskGroup.getString("name");
            tskGroup = TaskGroup.parse(taskGroup);
            existTaskGroups.put(tskGroup.name, tskGroup);
        }
        List<PostParam> params = Lists.newArrayList();

        if ((tskGroup = existTaskGroups.get(
                Objects.requireNonNull(groupName, "taskGroup.groupName can not be null"))) == null) {
            // 开始创建
            params.add(new PostParam(ExportTISPipelineToDolphinscheduler.FIELD_PROJECT_CODE, exportTISPipelineToDolphinscheduler.projectCode));
            tskGroup = this.processTISTaskGroup(endpoint, "create", params);
        } else {
            // 开始更新
            // 如果groupSize相同则不需要更新了
            if (Long.parseLong(exportTISPipelineToDolphinscheduler.projectCode) != tskGroup.projectCode) {
                throw TisException.create("exist same groupName,but the projectCode:" + tskGroup.projectCode + " is not equal the value you input in the form");
            }
            if (tskGroup.groupSize != parallelism) {
                params.add(new PostParam("id", tskGroup.id));
                tskGroup = this.processTISTaskGroup(endpoint, "update", params);
            }
        }
        return tskGroup;
    }

    private TaskGroup processTISTaskGroup(DolphinSchedulerEndpoint endpoint, String processType, List<PostParam> params) {
        if (StringUtils.isEmpty(processType)) {
            throw new IllegalArgumentException("param processType can not be empty");
        }


        params.add(new PostParam("name", this.groupName));
        params.add(new PostParam("description", "Task group for TIS data synchronize pipeline"));
        params.add(new PostParam("groupSize", this.parallelism));

        DolphinSchedulerResponse response = endpoint.createSchedulerURLBuilder()
                .appendSubPath("task-group", processType)
                .applyPost(params, Optional.empty());
        if (!response.isSuccess()) {
            throw new IllegalStateException(response.errorDescribe());
        }

        return TaskGroup.parse(response.getData());
    }

    public static class TaskGroup {
        private final String name;
        private final Integer id;
        private final Integer groupSize;
        private final long projectCode;

        private static TaskGroup parse(JSONObject taskGroup) {
            String taskGroupName = taskGroup.getString("name");
            long projectCode = taskGroup.getLongValue("projectCode");
            return new TaskGroup(taskGroupName, taskGroup.getInteger("id"), taskGroup.getInteger("groupSize"), projectCode);
        }

        public Integer getTaskGroupId() {
            return this.id;
        }

        public TaskGroup(String name, Integer id, Integer groupSize, long projectCode) {
            this.name = name;
            this.id = id;
            this.groupSize = groupSize;
            this.projectCode = projectCode;
        }
    }

    @TISExtension
    public static class DefaultDesc extends Descriptor<DSTaskGroup> {
        public DefaultDesc() {
            super();
        }

        @Override
        public String getDisplayName() {
            return "Default";
        }

        /**
         * 校验并发数目
         *
         * @param msgHandler
         * @param context
         * @param fieldName
         * @param value
         * @return
         */
        public boolean validateParallelism(IFieldErrorHandler msgHandler
                , Context context, String fieldName, String value) {
            int parallelism = Integer.parseInt(value);
            final int maxParallelism = 12;
            if (parallelism > maxParallelism) {
                msgHandler.addFieldError(context, fieldName, "执行并发数不能超过" + maxParallelism);
                return false;
            }

            return true;
        }

    }

}
