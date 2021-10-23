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

package com.qlangtech.tis.offline.flattable;

import com.google.common.collect.Maps;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.dump.INameWithPathGetter;
import com.qlangtech.tis.exec.ExecChainContextUtils;
import com.qlangtech.tis.fs.IFs2Table;
import com.qlangtech.tis.fs.ITISFileSystem;
import com.qlangtech.tis.fs.ITISFileSystemFactory;
import com.qlangtech.tis.fs.ITaskContext;
import com.qlangtech.tis.fullbuild.indexbuild.IDumpTable;
import com.qlangtech.tis.fullbuild.indexbuild.ITabPartition;
import com.qlangtech.tis.fullbuild.phasestatus.IJoinTaskStatus;
import com.qlangtech.tis.fullbuild.phasestatus.JobLog;
import com.qlangtech.tis.fullbuild.taskflow.AdapterTask;
import com.qlangtech.tis.fullbuild.taskflow.DataflowTask;
import com.qlangtech.tis.fullbuild.taskflow.ITemplateContext;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.offline.FlatTableBuilder;
import com.qlangtech.tis.order.center.IJoinTaskContext;
import com.qlangtech.tis.plugin.PluginStore;
import com.qlangtech.tis.sql.parser.IAliasTable;
import com.qlangtech.tis.sql.parser.ISqlTask;
import com.qlangtech.tis.sql.parser.TabPartitions;
import com.qlangtech.tis.sql.parser.er.ERRules;
import com.qlangtech.tis.sql.parser.er.IPrimaryTabFinder;
import com.qlangtech.tis.sql.parser.meta.DependencyNode;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import junit.framework.TestCase;
import org.apache.commons.io.IOUtils;
import org.easymock.EasyMock;

import java.io.InputStream;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author: baisui 百岁
 * @create: 2020-06-03 15:14
 **/
public class TestHiveFlatTableBuilder extends TestCase {


    private PluginStore<FlatTableBuilder> flatTableBuilderStore;

    @Override
    protected void setUp() throws Exception {
        this.flatTableBuilderStore = TIS.getPluginStore(FlatTableBuilder.class);
    }

    public void testTotalpaySummary() throws Exception {
        HiveFlatTableBuilder flatTableBuilder = (HiveFlatTableBuilder) flatTableBuilderStore.getPlugin();


//        ISqlTask nodeMeta, boolean isFinalNode
//            , ITemplateContext tplContext, ITaskContext taskContext, IFs2Table fs2Table, IJoinTaskStatus joinTaskStatus

        IFs2Table fs2Table = new MockFs2Table();
        IJoinTaskStatus joinTaskStatus = EasyMock.createMock("joinTaskStatus", IJoinTaskStatus.class);
        joinTaskStatus.setComplete(true);
        joinTaskStatus.createJobStatus(EasyMock.anyInt());
        JobLog jobLog = new JobLog();
        EasyMock.expect(joinTaskStatus.getJoblog(EasyMock.anyInt())).andReturn(jobLog).anyTimes();
        joinTaskStatus.setStart();


        IJoinTaskContext joinTaskContext = EasyMock.createMock("joinTaskContext", IJoinTaskContext.class);

        Map<IDumpTable, ITabPartition> dateParams = Maps.newHashMap();
        EasyMock.expect(joinTaskContext.getAttribute(ExecChainContextUtils.PARTITION_DATA_PARAMS)).andReturn(dateParams).anyTimes();
        Map<String, Boolean> taskWorkStatus = Maps.newHashMap();
        EasyMock.expect(joinTaskContext.getAttribute(AdapterTask.KEY_TASK_WORK_STATUS)).andReturn(taskWorkStatus);
        ERRules erRules = EasyMock.createMock("erRules", ERRules.class);

        EasyMock.expect(joinTaskContext.getAttribute("er_rules")).andReturn(erRules);

        EasyMock.replay(joinTaskStatus, joinTaskContext, erRules);

        MockTemplateContext tplContext = new MockTemplateContext(joinTaskContext);
        flatTableBuilder.startTask((context) -> {
            try (InputStream input = TestHiveFlatTableBuilder.class.getResourceAsStream("groupby_totalpay.sql")) {
                //     try (InputStream input = TestHiveFlatTableBuilder.class.getResourceAsStream("totalpay_summary.sql")) {
                ISqlTask sqlTask = new DefaultSqlTask(IOUtils.toString(input, TisUTF8.get()));
                DataflowTask joinTask = flatTableBuilder.createTask(sqlTask, true, tplContext, context, fs2Table, joinTaskStatus);
                joinTask.run();
            }
        });


    }

    private static class MockFs2Table implements IFs2Table {
        @Override
        public ITISFileSystem getFileSystem() {
            return null;
        }

        @Override
        public void bindTables(Set<EntityName> hiveTables, String timestamp, ITaskContext context) {

        }

        @Override
        public void deleteHistoryFile(EntityName dumpTable, ITaskContext taskContext) {

        }

        @Override
        public void deleteHistoryFile(EntityName dumpTable, ITaskContext taskContext, String timestamp) {

        }

        @Override
        public void dropHistoryTable(EntityName dumpTable, ITaskContext taskContext) {

        }

    }

    private static class MockTemplateContext implements ITemplateContext {
        private final IJoinTaskContext joinTaskContext;

        public MockTemplateContext(IJoinTaskContext joinTaskContext) {
            this.joinTaskContext = joinTaskContext;
        }

        @Override
        public <T> T getContextValue(String key) {
            throw new UnsupportedOperationException();
        }

        @Override
        public void putContextValue(String key, Object v) {
            throw new UnsupportedOperationException();
        }

        @Override
        public IJoinTaskContext getExecContext() {
            return joinTaskContext;
        }
    }

    private static class DefaultSqlTask implements ISqlTask {
        private final String sql;

        public DefaultSqlTask(String sql) {
            this.sql = sql;
        }

        @Override
        public String getSql() {
            return this.sql;
        }

        @Override
        public RewriteSql getRewriteSql(String taskName, TabPartitions dumpPartition
                , IPrimaryTabFinder erRules, ITemplateContext templateContext, boolean isFinalNode) {

            return new RewriteSql(sql, new MockAliasTable("1"));
        }

        @Override
        public String getId() {
            return "123";
        }

        @Override
        public String getExportName() {
            return "groupby_totalpay";
            //return "totalpay_summary";
        }

        @Override
        public List<DependencyNode> getDependencies() {
            return Collections.emptyList();
        }
    }

    private static class MockAliasTable implements IAliasTable {

        private final String newPT;

        public MockAliasTable(String newPT) {
            this.newPT = newPT;
        }

        @Override
        public IDumpTable getTable() {
            return null;
        }

        @Override
        public String getPt() {
            return this.newPT;
        }

        @Override
        public IAliasTable getChild() {
            return null;
        }

        @Override
        public boolean isSubQueryTable() {
            return false;
        }
    }
}