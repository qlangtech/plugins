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

package org.apache.log4j;

/**
 * 为了stub HoodieConfig 中创建logger的方法
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-04-20 18:44
 * @see org.apache.hudi.common.config.HoodieConfig
 **/
public class Logger {
    private org.slf4j.Logger logger;

    public Logger(org.slf4j.Logger logger) {
        this.logger = logger;
    }

    public void warn(String warn) {
        logger.warn(warn);
    }
}
