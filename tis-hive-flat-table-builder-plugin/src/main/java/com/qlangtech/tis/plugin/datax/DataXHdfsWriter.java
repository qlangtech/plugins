/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.tis.plugin.datax;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.hdfs.impl.HdfsFileSystemFactory;
import com.qlangtech.tis.offline.FileSystemFactory;
import com.qlangtech.tis.offline.flattable.HiveFlatTableBuilder;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;

/**
 * https://github.com/alibaba/DataX/blob/master/hdfswriter/doc/hdfswriter.md
 *
 * @author: baisui 百岁
 * @create: 2021-04-07 15:30
 **/
public class DataXHdfsWriter extends BasicFSWriter {
    private static final String DATAX_NAME = "Hdfs";

    @FormField(ordinal = 5, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.relative_path})
    public String path;


    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataXHdfsWriter.class, "DataXHdfsWriter-tpl.json");
    }

    @Override
    protected FSDataXContext getDataXContext(IDataxProcessor.TableMap tableMap) {
        return new HdfsDataXContext(tableMap, this.dataXName);
    }

    public class HdfsDataXContext extends FSDataXContext {
        public HdfsDataXContext(IDataxProcessor.TableMap tabMap, String dataxName) {
            super(tabMap, dataxName);
        }

        public String getPath() {
            return path;
        }
    }


    @TISExtension()
    public static class DefaultDescriptor extends BaseDataxWriterDescriptor {
        public DefaultDescriptor() {
            super();
            this.registerSelectOptions(HiveFlatTableBuilder.KEY_FIELD_NAME_FS_NAME
                    , () -> TIS.getPluginStore(FileSystemFactory.class).getPlugins());
        }


        public boolean validateFsName(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            return DataXHdfsWriter.validateFsName(msgHandler, context, fieldName, value);
        }

        @Override
        public boolean isRdbms() {
            return false;
        }

        @Override
        public String getDisplayName() {
            return DATAX_NAME;
        }
    }

    protected static boolean validateFsName(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
        FileSystemFactory fsFactory = FileSystemFactory.getFsFactory(value);
        if (fsFactory == null) {
            throw new IllegalStateException("can not find FileSystemFactory relevant with:" + value);
        }
        if (!(fsFactory instanceof HdfsFileSystemFactory)) {
            msgHandler.addFieldError(context, fieldName, "必须是HDFS类型的文件系统");
            return false;
        }
        return true;
    }
}
