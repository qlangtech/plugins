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

package com.qlangtech.tis.plugin.datax.tdfs.impl;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.tdfs.IDFSReader;
import com.qlangtech.tis.plugin.tdfs.ITDFSSession;
import com.qlangtech.tis.plugin.tdfs.TDFSLinker;
import com.qlangtech.tis.plugin.tdfs.TDFSSessionVisitor;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.util.AttrValMap;

import java.io.File;
import java.util.Collections;
import java.util.List;

/**
 * 使用本地文件作为资源
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-12-25 15:44
 **/
public class LocalFileDFSLinker extends TDFSLinker {
    private static final String LOCAL_FILE_DISPLAY_NAME = "Local Files";

    @Override
    public ITDFSSession createTdfsSession(Integer timeout) {
        return this.createTdfsSession();
    }

    @Override
    public ITDFSSession createTdfsSession() {
        return new LocalFileDFSSession(this);
    }

    @Override
    public <T> T useTdfsSession(TDFSSessionVisitor<T> tdfsSession) {
        try {
            return tdfsSession.accept(this.createTdfsSession());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @TISExtension
    public static class DftDescriptor extends BasicDescriptor {
        @Override
        public String getDisplayName() {
            return LOCAL_FILE_DISPLAY_NAME;
        }

        @Override
        protected List<IdentityName> createRefLinkers() {
            return Collections.emptyList();
        }

        public final boolean validatePath(IFieldErrorHandler msgHandler, Context context, String fieldName, String path) {
            File dirPath = new File(path);
            if (!dirPath.exists()) {
                msgHandler.addFieldError(context, fieldName, "该路径对应的资源目录不存在");
                return false;
            }
            if (!dirPath.isDirectory()) {
                msgHandler.addFieldError(context, fieldName, "路径须为目录，但现为文件资源");
                return false;
            }
            Descriptor currentRootPluginValidator = AttrValMap.getCurrentRootPluginValidator();
            if (IDFSReader.class.isAssignableFrom(currentRootPluginValidator.clazz)) {
                if (dirPath.list().length < 1) {
                    msgHandler.addFieldError(context, fieldName, "该路径为空，没有发现资源文件");
                    return false;
                }
            }


            return true;
        }

        @Override
        public final boolean validateLinker(IFieldErrorHandler msgHandler, Context context, String fieldName, String linker) {
            return true;
        }
    }
}
