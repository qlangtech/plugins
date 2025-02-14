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

package com.qlangtech.tis.plugin.common;

import com.alibaba.datax.common.util.Configuration;
import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.impl.IOUtils;
import org.apache.commons.lang.StringUtils;

import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-10-15 14:52
 **/
public abstract class DataXCfgJson {
    //    private final String val;
    private final boolean path;

    List<Function<Configuration, Configuration>> cfgSetters = Lists.newArrayList();

    public DataXCfgJson addCfgSetter(Function<Configuration, Configuration> setter) {
        this.cfgSetters.add(setter);
        return this;
    }

    private DataXCfgJson(String val, boolean path) {
//        this.val = val;
        this.path = path;
    }

    public static DataXCfgJson path(Class ownerClazz, String path) {
        return path(ownerClazz, path, (c) -> {
        });
    }

    public static DataXCfgJson path(Class ownerClazz, String path, Consumer<Configuration> postSet) {
        Configuration conf = IOUtils.loadResourceFromClasspath(
                ownerClazz, path, true
                , (writerJsonInput) -> {
                    Configuration c = Configuration.from(writerJsonInput);
                    return c;
                });
        postSet.accept(conf);
        return new DataXCfgJson(path, true) {
            @Override
            public Configuration getConfiguration() {
                return conf;
            }
        };
    }

    public static DataXCfgJson content(String content) {
        if (StringUtils.isEmpty(content)) {
            throw new IllegalArgumentException("param content can not be null");
        }
        return new DataXCfgJson(content, false) {
            @Override
            public Configuration getConfiguration() {
                Configuration conf = Configuration.from(content);
                return conf;
            }
        };
    }


    public abstract Configuration getConfiguration();

    public final String getVal() {
        return getConfiguration().beautify();
    }

    public boolean isPath() {
        return path;
    }
}
