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

package com.qlangtech.plugins.incr.flink.launch;

import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.manage.common.Option;
import org.apache.commons.lang3.EnumUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.HtmlFormatter;

import java.time.Duration;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-03-04 12:02
 **/
public class FlinkDescriptor<T extends Describable> extends Descriptor<T> {

    protected void addFieldDescriptor(String fieldName, ConfigOption<?> configOption) {
        Description desc = configOption.description();
        HtmlFormatter htmlFormatter = new HtmlFormatter();

        Object d = configOption.defaultValue();

        StringBuffer helperContent = new StringBuffer(htmlFormatter.format(desc));

        Class<?> targetClazz = configOption.getClazz();
        List<Option> opts = null;
        if (targetClazz == Duration.class) {
            if (d != null) {
                d = ((Duration) d).getSeconds();
            }
            helperContent.append("\n\n 单位：`秒`");
        } else if (targetClazz.isEnum()) {
            List<Enum> enums = EnumUtils.getEnumList((Class<Enum>) targetClazz);
            opts = enums.stream().map((e) -> new Option(e.name())).collect(Collectors.toList());
        } else if (targetClazz == Boolean.class) {
            opts = Lists.newArrayList(new Option("是", true), new Option("否", false));
        }

        this.addFieldDescriptor(fieldName, d, helperContent.toString(), Optional.ofNullable(opts));
    }

}
