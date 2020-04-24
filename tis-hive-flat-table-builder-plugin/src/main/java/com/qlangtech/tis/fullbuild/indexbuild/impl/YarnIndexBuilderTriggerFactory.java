/* * Copyright 2020 QingLang, Inc.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.qlangtech.tis.fullbuild.indexbuild.impl;

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.build.task.TaskMapper;
import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.config.yarn.IYarnConfig;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.fullbuild.indexbuild.IIndexBuildParam;
import com.qlangtech.tis.fullbuild.indexbuild.IRemoteJobTrigger;
import com.qlangtech.tis.fullbuild.indexbuild.TaskContext;
import com.qlangtech.tis.offline.FileSystemFactory;
import com.qlangtech.tis.offline.IndexBuilderTriggerFactory;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * @create: 2020-04-08 14:11
 *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2020/04/13
 */
public class YarnIndexBuilderTriggerFactory extends IndexBuilderTriggerFactory implements IContainerPodSpec {

    private static final Logger logger = LoggerFactory.getLogger(YarnIndexBuilderTriggerFactory.class);

    public static final String FIELD_CONTAINER_NAME = "containerName";

    public static final String FIELD_FS_NAME = "fsName";

    @FormField(ordinal = 0, validate = {Validator.require, Validator.identity})
    public String name;

    @FormField(ordinal = 1, validate = {Validator.require, Validator.identity}, type = FormFieldType.SELECTABLE)
    public String containerName;


    @FormField(ordinal = 2, validate = {Validator.require}, type = FormFieldType.INT_NUMBER, dftVal = "1024")
    public int maxHeapMemory;

    @FormField(ordinal = 3, validate = {Validator.require}, type = FormFieldType.INT_NUMBER, dftVal = "1")
    public int maxCPUCores;

    @FormField(ordinal = 4, type = FormFieldType.INT_NUMBER)
    public int runjdwpPort;

    @FormField(ordinal = 5, validate = {Validator.require, Validator.identity}, type = FormFieldType.SELECTABLE)
    public String fsName;

    @FormField(ordinal = 6, validate = {Validator.require}, type = FormFieldType.INT_NUMBER)
    public int maxDocMakeFaild;


    @Override
    public String getName() {
        return this.name;
    }

    private FileSystemFactory getFsFactory() {
        return FileSystemFactory.getFsFactory(this.fsName);
    }

    private IYarnConfig getYarnConfig() {
        return ParamsConfig.getItem(this.containerName, IYarnConfig.class);
    }

    @Override
    public IRemoteJobTrigger createBuildJob(String timePoint, String indexName
            , String groupNum, IIndexBuildParam buildParam, TaskContext context) throws Exception {
        Hadoop020RemoteJobTriggerFactory indexBuilderTriggerFactory
                = new Hadoop020RemoteJobTriggerFactory(getYarnConfig(), getFsFactory(), null);
        return indexBuilderTriggerFactory.createBuildJob(timePoint, indexName, groupNum, buildParam, context);
    }

    /**
     * 服务端开始执行任务
     *
     * @param taskMapper
     */
    public void startTask(TaskMapper taskMapper, final TaskContext taskContext) {
        ServerTaskExecutor taskExecutor = new ServerTaskExecutor(this.getYarnConfig());

       final DefaultCallbackHandler callbackHandler = new DefaultCallbackHandler() {
            @Override
            public float getProgress() {
//                if (indexBuilder == null || taskContext == null) {
//                    return 0;
//                }
                final long allRowCount = taskContext.getAllRowCount();
                long indexMakeCounter = taskContext.getIndexMakerComplete();
                float mainProgress = (float) (((double) indexMakeCounter) / allRowCount);
                return (float) (((mainProgress > 1.0) ? 1.0 : mainProgress));
            }
        };

        taskExecutor.startTask(taskMapper, taskContext, callbackHandler);
    }


    @Override
    public int getMaxHeapMemory() {
        return maxHeapMemory;
    }

    @Override
    public int getMaxCPUCores() {
        return maxCPUCores;
    }

    @Override
    public int getRunjdwpPort() {
        return runjdwpPort;
    }


    @TISExtension
    public static class DefaultDescriptor extends Descriptor<IndexBuilderTriggerFactory> {

        public DefaultDescriptor() {
            super();
            registerSelectOptions(FIELD_CONTAINER_NAME, () -> ParamsConfig.getItems(IYarnConfig.class));
            registerSelectOptions(FIELD_FS_NAME, () -> TIS.getPluginStore(FileSystemFactory.class).getPlugins());
        }

        @Override
        public String getDisplayName() {
            return "yarn";
        }
    }
}
