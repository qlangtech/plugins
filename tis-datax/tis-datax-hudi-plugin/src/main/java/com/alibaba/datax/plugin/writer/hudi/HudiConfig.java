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

package com.alibaba.datax.plugin.writer.hudi;

import org.apache.commons.lang.StringUtils;

import java.util.ResourceBundle;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-01-28 12:35
 **/
public class HudiConfig {
    private static HudiConfig config;

    private final String sparkPackageName;
    private final String sparkDistDirName;

    private HudiConfig() {
        ResourceBundle bundle =
                ResourceBundle.getBundle(StringUtils.replace(HudiConfig.class.getPackage().getName(), ".", "/") + "/config");
        sparkPackageName = bundle.getString("sparkPackageName");
        if (StringUtils.isEmpty(this.sparkPackageName)) {
            throw new IllegalStateException("config prop sparkPackageName can not be null");
        }
        sparkDistDirName =bundle.getString("sparkDistDirName");
    }

    public static String getSparkPackageName() {
        return getInstance().sparkPackageName;
    }

    public static String getSparkReleaseDir() {
        return getInstance().sparkDistDirName;
    }

    private static HudiConfig getInstance() {
        if (config == null) {
            synchronized (HudiConfig.class) {
                if (config == null) {
                    config = new HudiConfig();
                }
            }
        }
        return config;
    }
}
