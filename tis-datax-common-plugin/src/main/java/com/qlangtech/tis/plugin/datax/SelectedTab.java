/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.tis.plugin.datax;

import com.google.common.collect.Lists;
import com.qlangtech.tis.datax.ISelectedTab;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author: baisui 百岁
 * @create: 2021-04-08 13:29
 **/
public class SelectedTab implements ISelectedTab {
    private static final Logger logger = LoggerFactory.getLogger(SelectedTab.class);
    // 表名称
    @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String name;

    // 用户可以自己设置where条件
    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT)
    public String where;

    @FormField(ordinal = 2, type = FormFieldType.MULTI_SELECTABLE, validate = {Validator.require})
    public List<String> cols = Lists.newArrayList();

    private transient List<ColMeta> shadowCols = null;

    public SelectedTab(String name) {
        this.name = name;
    }

    public SelectedTab() {
    }

    /**
     * 取得默认的表名称
     *
     * @return
     */
    public static String getDftTabName() {
        DataxReader dataXReader = DataxReader.getThreadBingDataXReader();
        if (dataXReader == null) {
            return StringUtils.EMPTY;
        }

        try {
            List<ISelectedTab> selectedTabs = dataXReader.getSelectedTabs();
            for (ISelectedTab tab : selectedTabs) {
                return tab.getName();
            }
        } catch (Throwable e) {
            logger.warn(dataXReader.getDescriptor().getDisplayName() + e.getMessage());
        }

        return StringUtils.EMPTY;
    }

    public String getWhere() {
        return this.where;
    }


    public void setWhere(String where) {
        this.where = where;
    }

    public String getName() {
        return this.name;
    }

    public boolean isAllCols() {
        return this.cols.isEmpty();
    }

    public List<ColMeta> getCols() {
        if (shadowCols == null) {
            shadowCols = this.cols.stream().map((c) -> {
                ColMeta colMeta = new ColMeta();
                colMeta.setName(c);
                return colMeta;
            }).collect(Collectors.toList());
        }
        return shadowCols;
    }

    public boolean containCol(String col) {
        // return cols != null && this.cols.stream().filter((c) -> col.equals(c.getName())).findAny().isPresent();
        return cols != null && this.cols.contains(col);
    }

    public void setCols(List<String> cols) {
//        this.cols = cols.stream().map((c) -> {
//            ColMeta meta = new ColMeta();
//            meta.setName(c);
//            return meta;
//        }).collect(Collectors.toList());
        this.cols = cols;
    }


}
