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

package com.qlangtech.tis.plugin.ds.split;

import com.alibaba.citrus.turbine.Context;
import com.google.common.collect.Lists;
import com.qlangtech.tis.datax.DataXJobInfo;
import com.qlangtech.tis.datax.DataXJobSubmit;
import com.qlangtech.tis.db.parser.DBConfigParser;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.DBConfig;
import com.qlangtech.tis.plugin.ds.DBConfig.DBTable;
import com.qlangtech.tis.plugin.ds.DBConfig.HostDBs;
import com.qlangtech.tis.plugin.ds.DBIdentity;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.DefaultTab;
import com.qlangtech.tis.plugin.ds.SplitTableStrategy;
import com.qlangtech.tis.plugin.ds.SplitableTableInDB;
import com.qlangtech.tis.plugin.ds.SplitableTableInDB.SplitableDB;
import com.qlangtech.tis.plugin.ds.TableInDB;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

/**
 * 默认分表策略
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-12-17 21:23
 **/
public class DefaultSplitTableStrategy extends SplitTableStrategy {
    private static final Logger logger = LoggerFactory.getLogger(DefaultSplitTableStrategy.class);
    /**
     * 节点描述
     */
    @FormField(ordinal = 1, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String nodeDesc;

    @FormField(ordinal = 8, type = FormFieldType.INPUTTEXT, validate = {})
    public String tabPattern;

    @FormField(ordinal = 9, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String testTab;

    /**
     * prefixWildcardStyle 使用前缀匹配的样式，在flink-cdc表前缀通配匹配的场景中使用
     */
    @FormField(ordinal = 10, type = FormFieldType.ENUM, validate = {Validator.require})
    public Boolean prefixWildcardStyle;

    @Override
    public String getNodeDesc() {
        return this.nodeDesc;
    }

    @Override
    public boolean isSplittable() {
        return true;
    }

    @Override
    public TableInDB createTableInDB(DBIdentity id) {
        return new SplitableTableInDB(id
                , getTabPattern()
                , Objects.requireNonNull(this.prefixWildcardStyle, "prefixWildcardStyle can not be null"));
    }

    public static String tabPatternPlaceholder() {
        return SplitTableStrategy.PATTERN_PHYSICS_TABLE.toString();
    }

    private Pattern getTabPattern() {
        return StringUtils.isNotBlank(this.tabPattern) ? Pattern.compile(this.tabPattern) : SplitTableStrategy.PATTERN_PHYSICS_TABLE;
    }

    @Override
    public List<String> getAllPhysicsTabs(DataSourceFactory dsFactory, String jdbcUrl, String sourceTableName) {
        TableInDB tablesInDB = dsFactory.getTablesInDB();
        if (!(tablesInDB instanceof SplitableTableInDB)) {
            if (dsFactory instanceof IRefreshable) {
                ((IRefreshable) dsFactory).refresh();
            }
            tablesInDB = dsFactory.getTablesInDB();
            if (!(tablesInDB instanceof SplitableTableInDB)) {
                throw new IllegalStateException("dbId:" + dsFactory.identityValue()
                        + " relevant TableInDB must be " + SplitableTableInDB.class.getName() + ",but now is " + tablesInDB.getClass().getName());
            }
        }
        SplitableTableInDB tabsMapper = (SplitableTableInDB) tablesInDB;

        SplitableDB physics = tabsMapper.tabs.get(sourceTableName);
        if (physics != null) {

            List<String> ptabs = physics.getTabsInDB(jdbcUrl, false);
            if (CollectionUtils.isEmpty(ptabs)) {
                throw new IllegalStateException("jdbcUrl:" + jdbcUrl + "\n,logicTable:"
                        + sourceTableName + "\n,dsFactory:" + dsFactory.identityValue()
                        + "\n relevant physicsTab can not be empty"
                        + "\n exist keys:" + String.join(",", physics.physicsTabInSplitableDB.keySet()));
            }
            return ptabs;
        }
        throw new IllegalStateException(sourceTableName
                + " relevant physics tabs can not be null,exist keys:"
                + String.join(",", tabsMapper.getTabs()));
    }


    /**
     * 一个逻辑表如果对应有多个物理分区表，则只取第一个匹配的物理表
     *
     * @param dsFactory
     * @param tabName
     * @return
     */
    @Override
    public DBPhysicsTable getMatchedPhysicsTable(DataSourceFactory dsFactory, String jdbcUrl, EntityName tabName) {
        List<String> allPhysicsTabs = getAllPhysicsTabs(dsFactory, jdbcUrl, tabName.getTableName());
        for (String ptab : allPhysicsTabs) {
            return new DBPhysicsTable(jdbcUrl, EntityName.parse(ptab));
        }
        throw new IllegalStateException(tabName + " relevant physics tabs can not be null");
    }


    @TISExtension
    public static class DefatDesc extends Descriptor<SplitTableStrategy> {

        @Override
        public boolean secondVerify(IControlMsgHandler msgHandler
                , Context context, PostFormVals postFormVals, PostFormVals parentPostFormVals) {
            try {
                DefaultSplitTableStrategy splitTableStrategy = postFormVals.newInstance();
                if (StringUtils.isEmpty(splitTableStrategy.testTab)) {
                    throw new IllegalStateException("field testTab can not empty");
                }

                DataSourceFactory dataSource = parentPostFormVals.newInstance();
               // dataSource.refresh();
                DBConfig dbConfig = dataSource.getDbConfig();
                Map<String /*host*/, HostDBs> hostDBs = dbConfig.getHostDBsMapper();

                Function<DBTable, Stream<String>> tabnameCreator = (tab) -> {

                    TableInDB tabsInDB = dataSource.getTablesInDB();
                    DataXJobInfo jobInfo = tabsInDB.createDataXJobInfo(
                            DataXJobSubmit.TableDataXEntity.createTableEntity(null, tab.jdbcUrl, tab.getTabName()), false);
                    Optional<String[]> targetTableNames = jobInfo.getTargetTableNames();

                    List<String> physicsTabNames = targetTableNames
                            .map((tabNames) -> Lists.newArrayList(tabNames))
                            .orElseGet(() -> Lists.newArrayList());
                    return physicsTabNames.stream();

                };
                msgHandler.addActionMessage(context,"识别到的逻辑表与物理表映射，如下：");
                for (Map.Entry<String /**ip*/, HostDBs /**dbs*/> entry : hostDBs.entrySet()) {

                    entry.getValue().getDbStream().forEach((dbName) -> {
                        Set<String> tbs = entry.getValue().mapPhysicsTabs(
                                Collections.singletonMap(dbName, Collections.singletonList(new DefaultTab(splitTableStrategy.testTab)))
                                , tabnameCreator, false);
                        // 通过逻辑表取得在 jdbcurl-> database-> physicsTabs 下的物理表
                        msgHandler.addActionMessage(context, "host:" + entry.getKey() + ",db:" + dbName + ",matched tabs:" + String.join(",", tbs));
                    });

                }


            } catch (Exception e) {
                throw new RuntimeException(e);
            }
            return true;
        }

        public boolean validateNodeDesc(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {

            Map<String, List<String>> dbname = DBConfigParser.parseDBEnum("dbname", value);
            if (MapUtils.isEmpty(dbname)) {
                msgHandler.addFieldError(context, fieldName, "请确认格式是否正确");
                return false;
            }

            return true;
        }

        public boolean validateTabPattern(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            Pattern pattern = null;
            try {
                pattern = Pattern.compile(value);

            } catch (Exception e) {
                logger.warn(e.getMessage(), e);
                msgHandler.addFieldError(context, fieldName, e.getMessage());
                return false;
            }

            Matcher matcher = SplitableTableInDB.firstLogicTabNamePattern.matcher(pattern.pattern());
            int found = 0;
            while (matcher.find()) {
                found++;
            }
            if (found < 1) {
                msgHandler.addFieldError(context, fieldName, "正则式中没有发现识别逻辑表的组，请将代表逻辑表的部分用括号括起来");
                return false;
            }
            if (found > 2) {
                msgHandler.addFieldError(context, fieldName, "正则式中不能出现两个以上的识别组（用括号括起来的部分）");
                return false;
            }

            return true;
        }


        @Override
        public String getDisplayName() {
            return SWITCH_ON;
        }
    }
}
