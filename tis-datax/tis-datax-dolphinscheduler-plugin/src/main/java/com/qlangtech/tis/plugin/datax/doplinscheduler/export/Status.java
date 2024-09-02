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

package com.qlangtech.tis.plugin.datax.doplinscheduler.export;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-08-24 16:24
 * @see .//dolphinscheduler-api/src/main/java/org/apache/dolphinscheduler/api/enums/Status.java
 **/
public enum Status {
    PROJECT_NOT_EXIST(10190, "This project was not found. Please refresh page.", "该项目不存在,请刷新页面")
    , PROCESS_DEFINITION_NAME_EXIST(10168, "process definition name {0} already exists", "工作流定义名称[{0}]已存在")
    ,PROCESS_DEFINE_NOT_EXIST(50003, "process definition {0} does not exist", "工作流定义[{0}]不存在")
    ,PROCESS_DEFINE_NOT_RELEASE(50004, "process definition {0} process version {1} not online",
                                       "工作流定义[{0}] 工作流版本[{1}]不是上线状态");
    private final int code;
    private final String enMsg;
    private final String zhMsg;

    Status(int code, String enMsg, String zhMsg) {
        this.code = code;
        this.enMsg = enMsg;
        this.zhMsg = zhMsg;
    }

    public int getCode() {
        return this.code;
    }

//    public String getMsg() {
//        if (Locale.SIMPLIFIED_CHINESE.getLanguage().equals(LocaleContextHolder.getLocale().getLanguage())) {
//            return this.zhMsg;
//        } else {
//            return this.enMsg;
//        }
//    }
//

    /**
     * Retrieve Status enum entity by status code.
     */
    public static Status findStatusBy(int code) {
        for (Status status : Status.values()) {
            if (code == status.getCode()) {
                return status;
            }
        }
        throw new IllegalStateException("illegal code:" + code);
    }
}
