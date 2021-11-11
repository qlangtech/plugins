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

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.annotation.JSONField;
import com.google.common.collect.Lists;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-07-01 13:02
 **/
public class ParseColsResult {

    private static final Logger logger = LoggerFactory.getLogger(ParseColsResult.class);

    public DataXReaderTabMeta tabMeta;
    public boolean success;

    public static ParseColsResult parseColsCfg(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
        ParseColsResult parseOSSColsResult = new ParseColsResult();

        DataXReaderTabMeta tabMeta = new DataXReaderTabMeta();
        parseOSSColsResult.tabMeta = tabMeta;
        DataXColMeta colMeta = null;
        try {
            JSONArray cols = JSONArray.parseArray(value);
            if (cols.size() < 1) {
                msgHandler.addFieldError(context, fieldName, "请填写读取字段列表内容");
                return parseOSSColsResult;
            }
            Object firstElement = null;
            if (cols.size() == 1 && (firstElement = cols.get(0)) != null && "*".equals(String.valueOf(firstElement))) {
                tabMeta.allCols = true;
                return parseOSSColsResult.ok();
            }
            JSONObject col = null;
            String type = null;
            ColumnMetaData.DataType parseType = null;
            Integer index = null;
            String appValue = null;
            for (int i = 0; i < cols.size(); i++) {
                col = cols.getJSONObject(i);
                type = col.getString("type");
                if (StringUtils.isEmpty(type)) {
                    msgHandler.addFieldError(context, fieldName, "index为" + i + "的字段列中，属性type不能为空");
                    return parseOSSColsResult.faild();
                }
                parseType = ISelectedTab.DataXReaderColType.parse(type);
                if (parseType == null) {
                    msgHandler.addFieldError(context, fieldName, "index为" + i + "的字段列中，属性type必须为:" + ISelectedTab.DataXReaderColType.toDesc() + "中之一");
                    return parseOSSColsResult.faild();
                }

                colMeta = new DataXColMeta(parseType);
                tabMeta.cols.add(colMeta);
                index = col.getInteger("index");
                appValue = col.getString("value");

                if (index == null && appValue == null) {
                    msgHandler.addFieldError(context, fieldName, "index为" + i + "的字段列中，index/value必须选择其一");
                    return parseOSSColsResult.faild();
                }
                if (index != null) {
                    colMeta.index = index;
                }
                if (appValue != null) {
                    colMeta.value = appValue;
                }
            }
        } catch (Exception e) {
            logger.error(value, e);
            msgHandler.addFieldError(context, fieldName, "请检查内容格式是否有误:" + e.getMessage());
            return parseOSSColsResult.faild();
        }

        return parseOSSColsResult.ok();
    }

    public ParseColsResult ok() {
        this.success = true;
        return this;
    }

    public ParseColsResult faild() {
        this.success = false;
        return this;
    }

    static class DataXColMeta {
        public final ColumnMetaData.DataType parseType;

        // index和value两个属性为2选1
        private int index;
        private String value;

        public DataXColMeta(ColumnMetaData.DataType parseType) {
            this.parseType = parseType;
        }
    }

    public static class DataXReaderTabMeta implements ISelectedTab {
        public boolean allCols = false;
        public final List<DataXColMeta> cols = Lists.newArrayList();

        @Override
        @JSONField(serialize = false)
        public String getName() {
            return "oss"; // throw new UnsupportedOperationException();
        }

        @Override
        @JSONField(serialize = false)
        public String getWhere() {
            return StringUtils.EMPTY;
            // throw new UnsupportedOperationException();
        }

        @Override
        public boolean isAllCols() {
            return this.allCols;
        }

        @Override
        public List<ISelectedTab.ColMeta> getCols() {
            if (isAllCols()) {
                return Collections.emptyList();
            }
            return cols.stream().map((c) -> {
                ColMeta cmeta = new ColMeta();
                cmeta.setName(null);
                cmeta.setType(c.parseType);
                return cmeta;
            }).collect(Collectors.toList());
        }
    }
}
