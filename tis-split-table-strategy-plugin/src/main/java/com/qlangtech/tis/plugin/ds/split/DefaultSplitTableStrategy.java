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
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.ds.DBIdentity;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.SplitTableStrategy;
import com.qlangtech.tis.plugin.ds.TableInDB;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.regex.Pattern;

/**
 * 默认分表策略
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-12-17 21:23
 **/
public class DefaultSplitTableStrategy extends SplitTableStrategy {
    private static final Logger logger = LoggerFactory.getLogger(DefaultSplitTableStrategy.class);

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {})
    public String tabPattern;

    @Override
    public boolean isSplittable() {
        return true;
    }

    @Override
    public TableInDB createTableInDB(DBIdentity id) {
        return new SplitableTableInDB(id, getTabPattern());
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

            List<String> ptabs = physics.getTabsInDB(jdbcUrl);
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

        public boolean validateTabPattern(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {

            try {
                Pattern.compile(value);
            } catch (Exception e) {
                logger.warn(e.getMessage(), e);
                msgHandler.addFieldError(context, fieldName, e.getMessage());
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
