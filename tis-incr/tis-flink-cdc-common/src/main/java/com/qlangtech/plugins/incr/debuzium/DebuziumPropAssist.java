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

package com.qlangtech.plugins.incr.debuzium;

import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.util.AbstractPropAssist;
import com.qlangtech.tis.manage.common.Option;
import io.debezium.config.Field;
import io.debezium.config.Field.EnumRecommender;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-03-04 12:02
 **/
public class DebuziumPropAssist extends AbstractPropAssist<Describable, Field> {

    private DebuziumPropAssist(Descriptor descriptor) {
        super(descriptor);
    }

    public static <T extends Describable, PLUGIN extends T> Options<PLUGIN, Field> createOpts(Descriptor<T> descriptor) {
        DebuziumPropAssist flinkProps = new DebuziumPropAssist(descriptor);
        return flinkProps.createOptions();
    }

    @Override
    protected MarkdownHelperContent getDescription(Field configOption) {
        return new MarkdownHelperContent(configOption.description());
    }

    @Override
    protected Object getDefaultValue(Field configOption) {
        return configOption.defaultValue();
    }

    @Override
    protected List<Option> getOptEnums(Field configOption) {
        Type targetClazz = configOption.type();
        List<Option> opts = null;
        switch (targetClazz) {
            case LIST: {
                throw new IllegalStateException("unsupported type:" + targetClazz);
            }
            case BOOLEAN: {
                opts = Lists.newArrayList(new Option("是", true), new Option("否", false));
                break;
            }
            case CLASS:
            case PASSWORD:
            case INT:
            case DOUBLE:
            case LONG:
            case SHORT:
            case STRING:
            default:
                // throw new IllegalStateException("unsupported type:" + targetClazz);
        }

        if (configOption.recommender() instanceof EnumRecommender) {
            EnumRecommender enums = (EnumRecommender) configOption.recommender();
            List vals = enums.validValues(null, null);
            opts = (List<Option>) vals.stream().map((e) -> new Option(String.valueOf(e))).collect(Collectors.toList());
        }
        return opts;
    }

    @Override
    protected String getDisplayName(Field configOption) {
        return configOption.displayName();
    }
}
