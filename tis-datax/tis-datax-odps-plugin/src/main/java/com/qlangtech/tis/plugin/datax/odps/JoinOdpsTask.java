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

package com.qlangtech.tis.plugin.datax.odps;

import com.aliyun.odps.Instance;
import com.aliyun.odps.Instances;
import com.aliyun.odps.Odps;
import com.aliyun.odps.OdpsException;
import com.aliyun.odps.task.SQLTask;
import com.google.common.collect.Lists;
import com.qlangtech.tis.fullbuild.phasestatus.IJoinTaskStatus;
import com.qlangtech.tis.fullbuild.taskflow.HiveTask;
import com.qlangtech.tis.hive.AbstractInsertFromSelectParser;
import com.qlangtech.tis.plugin.ds.DataSourceMeta;
import com.qlangtech.tis.plugin.ds.IDataSourceFactoryGetter;
import com.qlangtech.tis.sql.parser.ISqlTask;
import com.qlangtech.tis.sql.parser.er.IPrimaryTabFinder;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.apache.commons.collections.map.HashedMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-02-21 16:33
 **/
public class JoinOdpsTask extends HiveTask {

    private static final Logger logger = LoggerFactory.getLogger(JoinOdpsTask.class);

    public JoinOdpsTask(IDataSourceFactoryGetter dsFactoryGetter,
                        ISqlTask nodeMeta, boolean isFinalNode
            , IPrimaryTabFinder erRules, IJoinTaskStatus joinTaskStatus) {
        super(dsFactoryGetter, nodeMeta, isFinalNode, erRules, joinTaskStatus);
    }

    @Override
    protected void executeSql(String sql, DataSourceMeta.JDBCConnection conn) throws SQLException {

        OdpsDataSourceFactory dsFactory
                = (OdpsDataSourceFactory) dsFactoryGetter.getDataSourceFactory();
        Instance ist = null;
       // Instance instance = null;
        Instance.Status status = null;
        Instance.TaskStatus ts = null;
        try {
            Odps odps = dsFactory.createOdps();
           // odps.projects().updateProject();
            ist = SQLTask.run(odps, sql);

           // Instances instances = odps.instances();
            TaskStatusCollection taskInfo = new TaskStatusCollection();

            while (true) {
                //instance = instances.get(dsFactory.project, ist.getId());
                taskInfo.update(ist);
                status = ist.getStatus();

                if (status == Instance.Status.TERMINATED) {
                    if (taskInfo.isFaild()) {
                        this.joinTaskStatus.setFaild(true);
                    }
                    break;
                } else {
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {

                    }
                }
            }

            logger.info("logViewUrl:{}", odps.logview().generateLogView(ist, 7 * 24));

        } catch (OdpsException e) {
            this.joinTaskStatus.setFaild(true);
            throw new SQLException(sql, e);
        } finally {
            this.joinTaskStatus.setComplete(true);
        }
    }

    private static class TaskStatusCollection {
        Map<String, TaskStatusInfo> taskInfo = new HashedMap();

        public void update(Instance instance) throws OdpsException {
            Instance.TaskStatus ts = null;
            TaskStatusInfo tskStatInfo = null;
            Map<String, Instance.TaskStatus> tStatus = instance.getTaskStatus();

            for (Map.Entry<String, Instance.TaskStatus> s : tStatus.entrySet()) {
                ts = s.getValue();
                if ((tskStatInfo = taskInfo.get(s.getKey())) != null) {
                    if (tskStatInfo.change(ts)) {
                        logger.info(instance.getTaskSummary(s.getKey()).getSummaryText());
                    }
                } else {
                    taskInfo.put(s.getKey(), new TaskStatusInfo(ts));
                }
            }
        }

        public boolean isFaild() {
            for (Map.Entry<String, TaskStatusInfo> status : taskInfo.entrySet()) {
                if (status.getValue().preState.get().getStatus() != Instance.TaskStatus.Status.SUCCESS) {
                    return true;
                }
            }
            return false;
        }
    }

    private static class TaskStatusInfo {
        private final AtomicReference<Instance.TaskStatus> preState;

        public TaskStatusInfo(Instance.TaskStatus preState) {
            this.preState = new AtomicReference<>(preState);
        }

        /**
         * 状态有变化
         *
         * @param ts
         * @return
         */
        public boolean change(Instance.TaskStatus ts) {
            Instance.TaskStatus pre = preState.get();
            if (pre.getStatus() != ts.getStatus()) {
                preState.set(ts);
                return true;
            }
            return false;
        }
    }

    @Override
    protected List<String> getHistoryPts(
            DataSourceMeta mrEngine, DataSourceMeta.JDBCConnection conn, EntityName table) throws Exception {
        return Lists.newArrayList();
    }

    @Override
    protected void initializeTable(DataSourceMeta mrEngine, ColsParser insertParser
            , DataSourceMeta.JDBCConnection conn, EntityName dumpTable, Integer partitionRetainNum) throws Exception {

    }

    @Override
    protected ResultSet convert2ResultSet(ResultSetMetaData metaData) throws SQLException {
        return null;
    }

    @Override
    protected AbstractInsertFromSelectParser createInsertSQLParser() {
        return new OdpsInsertFromSelectParser();
    }
}
