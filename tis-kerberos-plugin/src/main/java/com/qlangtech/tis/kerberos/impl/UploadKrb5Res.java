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
import com.qlangtech.tis.plugin.annotation.ITmpFileStore;
import com.qlangtech.tis.plugin.annotation.Validator;

import java.io.File;
import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-07-03 15:56
 **/
public class UploadKrb5Res extends Krb5Res implements ITmpFileStore {
    private static final String KEY_FILE = "file";
    @FormField(ordinal = 1, type = FormFieldType.FILE, validate = {Validator.require})
    public String file;

    private transient TmpFile tmp;

    @Override
    public boolean isKrb5PathNotNull() {
        return this.getTmpFile() != null;
    }

    private TmpFile getTmpFile() {
        return this.tmp;
    }

    @Override
    public File getKrb5Path() {
        return Objects.requireNonNull(this.getTmpFile(), "tmp file can not be null").tmp;
    }

    @Override
    public TmpFile getTmpeFile() {
        return this.tmp;
    }

    @Override
    public void setTmpeFile(TmpFile tmp) {
        this.tmp = tmp;
    }

    @Override
    public String getStoreFileName() {
        return Objects.requireNonNull(
                this.parentPluginId, "prop parentPluginId can not be null")
                .identityValue() + "_" + this.file;
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


    @TISExtension
    public static final class DftDescriptor extends BaseDescriptor {

        @Override
        public String getDisplayName() {
            return "Upload";
        }

        @Override
        protected String getResPropFieldName() {
            return KEY_FILE;
        }

//        @Override
//        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
//            Krb5Res krb5Res = postFormVals.newInstance(this, msgHandler);
//            File krb5Path = (krb5Res.getKrb5Path());
//            if (!krb5Path.exists()) {
//                msgHandler.addFieldError(context, KEY_FILE, NetUtils.getHostname() + "节点中不存在该路径");
//                return false;
//            }
//            String krb5Config = System.getProperty(KEY_KRB5_CONFIG);
//            try {
//                System.setProperty(KEY_KRB5_CONFIG, krb5Path.getAbsolutePath());
//                sun.security.krb5.Config.refresh();
//            } catch (KrbException e) {
//                logger.warn(e.getMessage(), e);
//                msgHandler.addFieldError(context, KEY_FILE, e.getMessage());
//                //  throw TisException.create(e.getMessage(), e);
//                return false;
//            } finally {
//                if (StringUtils.isNotBlank(krb5Config)) {
//                    System.setProperty(KEY_KRB5_CONFIG, krb5Config);
//                } else {
//                    System.clearProperty(KEY_KRB5_CONFIG);
//                }
//            }
//            return true;
//        }
    }

}
