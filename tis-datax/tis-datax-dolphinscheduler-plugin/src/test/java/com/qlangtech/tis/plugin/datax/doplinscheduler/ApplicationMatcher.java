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
import org.mockito.ArgumentMatcher;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-08-22 13:28
 **/
public class ApplicationMatcher implements ArgumentMatcher<Application> {
    private final String expectAppFullBuildCronTime;
    private Application actualApp;

    public ApplicationMatcher(String expectAppFullBuildCronTime) {
        this.expectAppFullBuildCronTime = expectAppFullBuildCronTime;
    }

    @Override
    public boolean matches(Application actual) {
        if (actual instanceof Application) {
            this.actualApp = actual;
            return StringUtils.equals(this.actualApp.getFullBuildCronTime(), expectAppFullBuildCronTime);
        }
        return false;
    }

    @Override
    public String toString() {
        return "a app with the expected value expectAppFullBuildCronTime:" + expectAppFullBuildCronTime
                + "\nis not equal actual:\n"
                + (this.actualApp == null ? null : this.actualApp.getFullBuildCronTime());
    }
}
