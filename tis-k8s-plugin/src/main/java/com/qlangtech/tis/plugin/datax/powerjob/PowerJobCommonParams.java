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

package com.qlangtech.tis.plugin.datax.powerjob;

import org.apache.commons.lang3.StringUtils;

import java.util.ResourceBundle;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-03-02 17:38
 **/
public class PowerJobCommonParams {
    private final String powerJobVersion;
    private static final Pattern pattern = Pattern.compile("[\\d\\.]+");
    private static PowerJobCommonParams param = new PowerJobCommonParams();

    private PowerJobCommonParams() {
        ResourceBundle bundle = ResourceBundle.getBundle("com/qlangtech/tis/plugin/datax/powerjob/config");
        this.powerJobVersion = bundle.getString("powerjob.version");
        if (StringUtils.isEmpty(this.powerJobVersion)) {
            throw new IllegalStateException("prop powerJobVersion can not be null");
        }
        Matcher matcher = pattern.matcher(this.powerJobVersion);
        if (!matcher.matches()) {
            throw new IllegalStateException("powerJobVersion:" + powerJobVersion + " must match pattern:" + pattern);
        }
    }

    public static String getPowerJobVersion() {
        return param.powerJobVersion;
    }
}
