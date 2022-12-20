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

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.SplitTableStrategy;
import com.qlangtech.tis.plugin.ds.TableInDB;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * 默认分表策略
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-12-17 21:23
 **/
public class DefaultSplitTableStrategy extends SplitTableStrategy {
    //private SplitableTableInDB logicTab2PhysicsMapper;

    @Override
    public TableInDB createTableInDB() {
        return new SplitableTableInDB();
    }

    // private static transient LoadingCache<String, SplitableTableInDB> tabsInDBCache;

//    @Override
//    public List<String> getAllPhysicsTabs(DataSourceFactory dsFactory, DataXJobSubmit.TableDataXEntity tabEntity) {
//        return getAllPhysicsTabs(dsFactory, tabEntity.getDbIdenetity(), tabEntity.getSourceTableName());
//    }


    @Override
    public List<String> getAllPhysicsTabs(DataSourceFactory dsFactory, String jdbcUrl, String sourceTableName) {
        TableInDB tablesInDB = dsFactory.getTablesInDB();// tabsInDBCache.get(dsFactory.identityValue());
        if (!(tablesInDB instanceof SplitableTableInDB)) {
            dsFactory.refresh();
            tablesInDB = dsFactory.getTablesInDB();
            if (!(tablesInDB instanceof SplitableTableInDB)) {
                throw new IllegalStateException("dbId:" + dsFactory.identityValue()
                        + " relevant TableInDB must be " + SplitableTableInDB.class.getName()
                        + ",but now is " + tablesInDB.getClass().getName());
            }
        }
        SplitableTableInDB tabsMapper = (SplitableTableInDB) tablesInDB;

        SplitableDB physics = tabsMapper.tabs.get(sourceTableName);
        if (physics != null) {
//            for (String ptab : ) {
//                return new DBPhysicsTable(jdbcUrl, EntityName.parse(ptab));
//            }
            return physics.getTabsInDB(jdbcUrl);
        }
        throw new IllegalStateException(sourceTableName + " relevant physics tabs can not be null");
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
        //try {
//        TableInDB tablesInDB = dsFactory.getTablesInDB();// tabsInDBCache.get(dsFactory.identityValue());
//        if (!(tablesInDB instanceof SplitableTableInDB)) {
//            dsFactory.refresh();
//            tablesInDB = dsFactory.getTablesInDB();
//            if (!(tablesInDB instanceof SplitableTableInDB)) {
//                throw new IllegalStateException("dbId:" + dsFactory.identityValue()
//                        + " relevant TableInDB must be " + SplitableTableInDB.class.getName()
//                        + ",but now is " + tablesInDB.getClass().getName());
//            }
//        }
//        SplitableTableInDB tabsMapper = (SplitableTableInDB) tablesInDB;
        List<String> allPhysicsTabs = getAllPhysicsTabs(dsFactory, jdbcUrl, tabName.getTableName());
        for (String ptab : allPhysicsTabs) {
            return new DBPhysicsTable(jdbcUrl, EntityName.parse(ptab));
        }
        throw new IllegalStateException(tabName + " relevant physics tabs can not be null");
    }

    public static class SplitableDB {
        // key:jdbcUrl ,val:physicsTab
        private Map<String, List<String>> physicsTabInSplitableDB = Maps.newHashMap();

        public SplitableDB add(String jdbcUrl, String tab) {
            List<String> tabs = physicsTabInSplitableDB.get(jdbcUrl);
            if (tabs == null) {
                tabs = Lists.newArrayList();
                physicsTabInSplitableDB.put(jdbcUrl, tabs);
            }
            tabs.add(tab);
            return this;
        }

        public List<String> getTabsInDB(String jdbcUrl) {
            return this.physicsTabInSplitableDB.get(jdbcUrl);
        }
    }

    public static class SplitableTableInDB extends TableInDB {
        //<key:逻辑表名,List<String> 物理表列表>
        public Map<String, SplitableDB> tabs = new HashMap<>();


        private static final Pattern PATTERN_PHYSICS_TABLE = Pattern.compile("(\\S+)_(\\d+)");

        /**
         * @param jdbcUrl 可以标示是哪个分库的
         * @param tab
         */
        @Override
        public void add(String jdbcUrl, String tab) {
            Matcher matcher = PATTERN_PHYSICS_TABLE.matcher(tab);
            SplitableDB physicsTabs = null;
            if (matcher.matches()) {
                String logicTabName = matcher.group(1);
                physicsTabs = tabs.get(logicTabName);
                if (physicsTabs == null) {
                    physicsTabs = new SplitableDB();
                    tabs.put(logicTabName, physicsTabs);
                }
                physicsTabs.add(jdbcUrl, tab);
            } else {
                tabs.put(tab, (new SplitableDB()).add(jdbcUrl, tab));
            }
        }

        @Override
        public List<String> getTabs() {
            return Lists.newArrayList(tabs.keySet());
        }

        @Override
        public boolean contains(String tableName) {
            return this.tabs.containsKey(tableName);
        }

        @Override
        public boolean isEmpty() {
            return this.tabs.isEmpty();
        }
    }

    @TISExtension
    public static class DefatDesc extends Descriptor<SplitTableStrategy> {
        @Override
        public String getDisplayName() {
            return SWITCH_ON;
        }
    }
}
