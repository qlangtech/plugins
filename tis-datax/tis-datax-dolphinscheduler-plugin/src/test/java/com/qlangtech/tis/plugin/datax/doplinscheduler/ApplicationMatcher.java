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

package com.qlangtech.tis.plugin.datax.doplinscheduler;

import com.qlangtech.tis.manage.biz.dal.pojo.Application;
import org.apache.commons.lang3.StringUtils;
import org.easymock.IArgumentMatcher;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-08-22 13:28
 **/
public class ApplicationMatcher implements IArgumentMatcher {
    private final String expectAppFullBuildCronTime;
    private Application actualApp;

    public ApplicationMatcher(String expectAppFullBuildCronTime) {
        this.expectAppFullBuildCronTime = expectAppFullBuildCronTime;
    }

    @Override
    public boolean matches(Object actual) {
        if (actual instanceof Application) {
            this.actualApp = (Application) actual;
            return StringUtils.equals(this.actualApp.getFullBuildCronTime(), expectAppFullBuildCronTime);
        }
        return false;
    }

    @Override
    public void appendTo(StringBuffer buffer) {
        buffer.append("a app with the expected value expectAppFullBuildCronTime:" + expectAppFullBuildCronTime);
        buffer.append("\nis not equal actual:\n");
        buffer.append(this.actualApp.getFullBuildCronTime());
    }
}
