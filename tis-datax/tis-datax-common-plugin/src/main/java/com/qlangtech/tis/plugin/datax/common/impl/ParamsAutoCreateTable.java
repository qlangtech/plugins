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

package com.qlangtech.tis.plugin.datax.common.impl;

import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.CreateTableSqlBuilder.ColWrapper;
import com.qlangtech.tis.plugin.datax.common.AutoCreateTable;
import com.qlangtech.tis.plugin.datax.common.AutoCreateTableColCommentSwitch;

import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-12-14 11:32
 **/
public abstract class ParamsAutoCreateTable<COL_WRAPPER extends ColWrapper> extends AutoCreateTable<COL_WRAPPER>  {
    /**
     * 添加列注释
     */
    @FormField(ordinal = 0, validate = {Validator.require})
    public AutoCreateTableColCommentSwitch addComment;

    @Override
    public boolean enabled() {
        return true;
    }

    @Override
    public AutoCreateTableColCommentSwitch getAddComment() {
        return Objects.requireNonNull(addComment, "addComment can not be null");
    }

    @Override
    public boolean enabledColumnComment() {
        return Objects.requireNonNull(this.addComment, "addComment can not be null").turnOn();
    }

    //  @TISExtension
    public abstract static class DftDesc extends BasicDescriptor {
        public DftDesc() {
            super();
        }

        @Override
        public final String getDisplayName() {
            return SWITCH_ON;
        }
    }
}
