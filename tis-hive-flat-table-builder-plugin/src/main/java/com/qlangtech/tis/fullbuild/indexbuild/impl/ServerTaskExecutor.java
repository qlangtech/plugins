/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 *   This program is free software: you can use, redistribute, and/or modify
 *   it under the terms of the GNU Affero General Public License, version 3
 *   or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 *  This program is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *   FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.tis.fullbuild.indexbuild.impl;

import com.qlangtech.tis.build.task.TaskMapper;
import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.config.yarn.IYarnConfig;
import com.qlangtech.tis.fullbuild.indexbuild.TaskContext;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.hadoop.yarn.api.records.FinalApplicationStatus;
import org.apache.hadoop.yarn.client.api.AMRMClient;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author: baisui 百岁
 * @create: 2020-04-23 20:20
 **/
public class ServerTaskExecutor {
    private static final Logger logger = LoggerFactory.getLogger(ServerTaskExecutor.class);
    //  private final IYarnConfig yarnConfig;

    private final YarnConfiguration conf;

    public ServerTaskExecutor(IYarnConfig yarnConfig) {
        this.conf = ((ParamsConfig) yarnConfig).createConfigInstance();
    }

    /**
     * 服务端开始执行任务
     *
     * @param taskMapper
     */
    public void startTask(TaskMapper taskMapper, TaskContext taskContext, AMRMClientAsync.CallbackHandler callbackHandler) throws Exception {
        AMRMClientAsync<AMRMClient.ContainerRequest> rmClient = null;
        try {


            //  DefaultCallbackHandler callbackHandler = new DefaultCallbackHandler();
            rmClient = AMRMClientAsync.createAMRMClientAsync(1000, callbackHandler);
            rmClient.init(conf);
            rmClient.start();

            rmClient.registerApplicationMaster("", 0, "");
            logger.info("have register master");

            // TaskReturn result =
            taskMapper.map(taskContext);

//            /* 执行索引build start */
//            HdfsIndexGetConfig configJob = new HdfsIndexGetConfig();
//            this.indexBuilder = new HdfsIndexBuilder();
//            TaskReturn result = configJob.map(taskContext);
//            if (result.getReturnCode() == ReturnCode.FAILURE) {
//                masterShutdown(FinalApplicationStatus.FAILED, result.getMsg());
//                return;
//            }
//            result = indexBuilder.map(taskContext);
//            if (result.getReturnCode() == TaskReturn.ReturnCode.FAILURE) {
//                masterShutdown(rmClient, FinalApplicationStatus.FAILED, result.getMsg());
//                return;
//            }

            /* 执行索引build end */
            masterShutdown(rmClient, FinalApplicationStatus.SUCCEEDED, StringUtils.EMPTY);
        } catch (Throwable e) {
            masterShutdown(rmClient, FinalApplicationStatus.FAILED, ExceptionUtils.getRootCauseMessage(e));
            throw new Exception(e);
        } finally {
            try {
                rmClient.close();
            } catch (Throwable e) {
                logger.error(e.getMessage(), e);
            }
            //  AppnameAwareFlumeLogstashV1Appender.closeAllFlume();
        }
    }

    protected void masterShutdown(AMRMClientAsync<AMRMClient.ContainerRequest> rmClient, FinalApplicationStatus appStatus, String msg) {
        String m = "build master application shutdown.";
        System.out.println(m);

        if (appStatus == FinalApplicationStatus.FAILED) {
            logger.error(m + ",status:" + appStatus + ",msg:" + msg);
        } else {
            logger.info(m + ",status:" + appStatus + ",msg:" + msg);
        }
        try {
            rmClient.unregisterApplicationMaster(appStatus
                    , msg, "");
        } catch (Exception exc) {
            // safe to ignore ... this usually fails anyway
            logger.error(exc.getMessage(), exc);
        }
    }
}
