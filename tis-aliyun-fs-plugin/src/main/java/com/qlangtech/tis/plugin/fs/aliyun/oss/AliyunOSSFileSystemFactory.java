/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 *   This program is free software: you can use, redistribute, and/or modify
 *   it under the terms of the GNU Affero General Public License, version 3
 *   or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 *  This program is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *   FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package com.qlangtech.tis.plugin.fs.aliyun.oss;

import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.config.aliyun.IAliyunToken;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.fs.ITISFileSystem;
import com.qlangtech.tis.offline.FileSystemFactory;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;

/**
 * 基于阿里云OSS的
 *
 * @author 百岁（baisui@qlangtech.com）
 * @create: 2020-04-12 20:03
 * @date 2020/04/13
 */
public class AliyunOSSFileSystemFactory extends FileSystemFactory {

    @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
    public String name;

    @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String rootDir;

    @FormField(ordinal = 3, type = FormFieldType.SELECTABLE, validate = {Validator.require, Validator.identity})
    public String aliyunToken;

    //example: http://oss-cn-hangzhou.aliyuncs.com
    @FormField(ordinal = 3, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.url})
    public String endpoint;

    @FormField(ordinal = 4, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String bucketName;


    private ITISFileSystem ossFs;

    @Override
    public ITISFileSystem getFileSystem() {
        if (ossFs == null) {
            IAliyunToken aliyunToken = ParamsConfig.getItem(this.aliyunToken, IAliyunToken.class);
            ossFs = new AliyunOSSFileSystem(aliyunToken, this.endpoint, this.bucketName, this.rootDir);
        }
        return ossFs;
    }

    @TISExtension
    public static class DefaultDescriptor extends Descriptor<FileSystemFactory> {

        public DefaultDescriptor() {
            super();
            this.registerSelectOptions(IAliyunToken.KEY_FIELD_ALIYUN_TOKEN, () -> ParamsConfig.getItems(IAliyunToken.class));
        }
    }
}
