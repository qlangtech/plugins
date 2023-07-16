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

package com.qlangtech.tis.kerberos;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.config.kerberos.IKerberos;
import com.qlangtech.tis.config.kerberos.Krb5Res;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.ITmpFileStore;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;

import java.io.File;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-06-01 09:27
 **/
public class KerberosCfg extends ParamsConfig implements IKerberos, ITmpFileStore {

    @FormField(identity = true, ordinal = 0, validate = {Validator.require, Validator.identity})
    public String name;

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String principal;

    @FormField(ordinal = 2, type = FormFieldType.FILE, validate = {Validator.require})
    public String keytabPath;

    @FormField(ordinal = 3, validate = {Validator.require})
    public Krb5Res krb5Res;

    private transient TmpFile tmp;

    public String getPrincipal() {
        return principal;
    }

    public String getKeytabPath() {
        return keytabPath;
    }

    @Override
    public String getStoreFileName() {
        return this.name + "_" + this.keytabPath;
    }

    public File getKeyTabPath() {
        return Objects.requireNonNull(tmp, "tmp file can not be null").tmp;
    }

    @Override
    public Krb5Res getKrb5Res() {
        return this.krb5Res;
    }

    @Override
    public void setTmpeFile(TmpFile tmp) {
        this.tmp = tmp;
    }

    @Override
    public TmpFile getTmpeFile() {
        return this.tmp;
    }

//    @Override
//    public void save(File parentDir) {
//        if (tmp == null) {
//            // 更新流程保持不变
//            File cfg = new File(parentDir, this.getStoreFileName());
//            if (!cfg.exists()) {
//                throw new IllegalStateException("cfg file is not exist:" + cfg.getAbsolutePath());
//            }
//            tmp = new TmpFile(cfg) {
//                @Override
//                public void saveToDir(File dir, String fileName) {
//                    throw new UnsupportedOperationException("fileName can not be replace:" + cfg.getAbsolutePath());
//                }
//            };
//        } else {
//            tmp.saveToDir(parentDir, this.getStoreFileName());
//        }
//    }

    @Override
    public Object createConfigInstance() {
        throw new UnsupportedOperationException();
    }

    @Override
    public String identityValue() {
        return this.name;
    }


    @TISExtension
    public static class DefaultDescriptor extends Descriptor<ParamsConfig> {
        private static final Pattern PATTERN_Principal = Pattern.compile(".+?@.+?");

        @Override
        public String getDisplayName() {
            return IKerberos.IDENTITY;
        }

//        @Override
//        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals vals) {
//
//            ParamsConfig paramsConfig = vals.newInstance(this, msgHandler);
//
//
//            return true;
//        }

        /**
         * format must be : username/fully.qualified.domain.name@YOUR_REALM.COM
         *
         * @param msgHandler
         * @param context
         * @param fieldName
         * @param value
         * @return
         */
        public boolean validatePrincipal(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            Matcher matcher = PATTERN_Principal.matcher(value);
            if (!matcher.matches()) {
                msgHandler.addFieldError(context, fieldName, "格式必须为:" + PATTERN_Principal.toString());
                return false;
            }
            return true;
        }
    }

}
