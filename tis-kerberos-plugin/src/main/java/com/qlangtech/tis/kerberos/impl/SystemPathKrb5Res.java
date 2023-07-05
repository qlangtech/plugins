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

package com.qlangtech.tis.kerberos.impl;

import com.qlangtech.tis.config.kerberos.Krb5Res;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

import java.io.File;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-07-03 15:56
 **/
public class SystemPathKrb5Res extends Krb5Res {
    private static final String KEY_PATH = "path";
    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.absolute_path})
    public String path;

    @Override
    public boolean isKrb5PathNotNull() {
        return true;
    }

    @Override
    public File getKrb5Path() {
        return new File(this.path);
    }

    @TISExtension
    public static final class DftDescriptor extends BaseDescriptor {
        @Override
        public String getDisplayName() {
            return "SystemPath";
        }

        @Override
        protected String getResPropFieldName() {
            return KEY_PATH;
        }

//        @Override
//        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
//            Krb5Res krb5Res = postFormVals.newInstance(this, msgHandler);
//            File krb5Path = (krb5Res.getKrb5Path());
//            if (!krb5Path.exists()) {
//                msgHandler.addFieldError(context, KEY_PATH, NetUtils.getHostname() + "节点中不存在该路径");
//                return false;
//            }
//            return true;
//        }
    }

}
