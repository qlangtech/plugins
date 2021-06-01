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
import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.config.hive.IHiveConnGetter;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.offline.FileSystemFactory;
import com.qlangtech.tis.offline.flattable.HiveFlatTableBuilder;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;

import java.sql.Connection;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-23 14:48
 **/
public class DataXHiveWriter extends BasicFSWriter {
    private static final String DATAX_NAME = "Hive";
    private static final String KEY_FIELD_NAME_HIVE_CONN = "hiveConn";

    @FormField(ordinal = 1, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String hiveConn;
    @FormField(ordinal = 2, type = FormFieldType.INT_NUMBER, validate = {Validator.require})
    public Integer partitionRetainNum;

    @Override
    protected FSDataXContext getDataXContext(IDataxProcessor.TableMap tableMap) {
        return new HiveDataXContext(tableMap, this.dataXName);
    }


    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataXHdfsWriter.class, "DataXHiveWriter-tpl.json");
    }

    public Connection getConnection() {
        try {
            ParamsConfig connGetter = (ParamsConfig) getHiveConnGetter();
            return connGetter.createConfigInstance();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    public IHiveConnGetter getHiveConnGetter() {
        return ParamsConfig.getItem(this.hiveConn, IHiveConnGetter.class);
    }

    public class HiveDataXContext extends FSDataXContext {

        public HiveDataXContext(IDataxProcessor.TableMap tabMap, String dataXName) {
            super(tabMap, dataXName);
        }

        public Integer getPartitionRetainNum() {
            return partitionRetainNum;
        }
    }

    @Override
    public String getTemplate() {
        return template;
    }

    @TISExtension()
    public static class DefaultDescriptor extends BaseDataxWriterDescriptor {
        public DefaultDescriptor() {
            super();
            this.registerSelectOptions(KEY_FIELD_NAME_HIVE_CONN, () -> ParamsConfig.getItems(IHiveConnGetter.class));
            this.registerSelectOptions(HiveFlatTableBuilder.KEY_FIELD_NAME_FS_NAME
                    , () -> TIS.getPluginStore(FileSystemFactory.class).getPlugins());
        }

        public boolean validatePartitionRetainNum(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            Integer retainNum = Integer.parseInt(value);
            if (retainNum < 1 || retainNum > 5) {
                msgHandler.addFieldError(context, fieldName, "数目必须为不小于1且不大于5之间");
                return false;
            }
            return true;
        }

        public boolean validateFsName(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            return DataXHdfsWriter.validateFsName(msgHandler, context, fieldName, value);
        }

//        @Override
//        protected boolean validate(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
//            return HiveFlatTableBuilder.validateHiveAvailable(msgHandler, context, postFormVals);
//        }

        @Override
        public boolean isRdbms() {
            return false;
        }

        @Override
        public String getDisplayName() {
            return DATAX_NAME;
        }
    }
}
