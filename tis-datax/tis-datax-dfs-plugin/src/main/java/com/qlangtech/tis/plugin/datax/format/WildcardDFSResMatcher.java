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

package com.qlangtech.tis.plugin.datax.format;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.tdfs.DFSResMatcher;
import com.qlangtech.tis.plugin.tdfs.ITDFSSession;
import org.apache.commons.io.FilenameUtils;

/**
 * dfs 资源名称 查找
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-08-09 09:56
 **/
public class WildcardDFSResMatcher extends DFSResMatcher {

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.relative_path})
    public String wildcard;

    @Override
    public String toString() {
        return wildcard;
    }

    @Override
    public boolean isMatch(ITDFSSession.Res testRes) {
        return FilenameUtils.wildcardMatch(testRes.relevantPath, this.wildcard);
    }


    @TISExtension
    public static class DftDescriptor extends Descriptor<DFSResMatcher> {
        @Override
        public String getDisplayName() {
            return "Wildcard";
        }
    }

}
