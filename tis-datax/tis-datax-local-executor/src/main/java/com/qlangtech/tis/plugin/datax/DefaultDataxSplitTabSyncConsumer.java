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

package com.qlangtech.tis.plugin.datax;

import com.qlangtech.tis.datax.DataxSplitTabSyncConsumer;
import com.qlangtech.tis.exec.IExecChainContext;
import org.apache.commons.lang3.StringUtils;

import java.io.File;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-04-13 15:54
 **/
public class DefaultDataxSplitTabSyncConsumer extends DataxSplitTabSyncConsumer {
    private final LocalDataXJobSubmit localDataXJobSubmit;

    public DefaultDataxSplitTabSyncConsumer(IExecChainContext execContext, LocalDataXJobSubmit localDataXJobSubmit) {
        super(execContext);
        this.localDataXJobSubmit = localDataXJobSubmit;
    }

    @Override
    protected String getClasspath() {
        return this.localDataXJobSubmit.getClasspath();
    }

    @Override
    protected final String[] getExtraJavaSystemPrams() {
        String[] extraJavaSystemPrams = localDataXJobSubmit.getExtraJavaSystemPrams();
        String javaMemory = execContext.getJavaMemSpec();//.getTaskContext().getJavaMemSpec();
        if (StringUtils.isNotEmpty(javaMemory)) {
            // 将内存规格拷贝到新的参数数组里
            String[] params = new String[extraJavaSystemPrams.length + 1];
            params[0] = javaMemory;
            System.arraycopy(extraJavaSystemPrams, 0, params, 1, extraJavaSystemPrams.length);
            return params;
        } else {
            return extraJavaSystemPrams;
        }
    }

    @Override
    protected String getMainClassName() {
        return localDataXJobSubmit.getMainClassName();
    }

    @Override
    protected File getWorkingDirectory() {
        return localDataXJobSubmit.getWorkingDirectory();
    }
}
