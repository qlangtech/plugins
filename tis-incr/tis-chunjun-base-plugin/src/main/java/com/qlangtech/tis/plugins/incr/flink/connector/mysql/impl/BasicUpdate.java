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

package com.qlangtech.tis.plugins.incr.flink.connector.mysql.impl;

import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugins.incr.flink.connector.mysql.UpdateMode;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-07-18 10:40
 **/
public abstract class BasicUpdate extends UpdateMode {
    @FormField(ordinal = 1, type = FormFieldType.ENUM, validate = {Validator.require})
    public List<String> updateKey;

    @Override
    public void set(Map<String, Object> params) {
        super.set(params);
        if (CollectionUtils.isEmpty(updateKey)) {
            throw new IllegalStateException("collection of 'updateKey' can not be null");
        }
        params.put("uniqueKey", this.updateKey);
    }

    /**
     * 主键候选字段
     *
     * @return
     */
    public static List<Option> getPrimaryKeys() {
        return SelectedTab.getContextTableCols((cols) -> cols.stream().filter((col) -> col.isPk()));
    }

    public static List<String> getDeftRecordKeys() {
        return getPrimaryKeys().stream()
                .map((pk) -> String.valueOf(pk.getValue())).collect(Collectors.toList());
    }


}
