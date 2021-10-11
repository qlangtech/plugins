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
import com.alibaba.citrus.turbine.impl.DefaultContext;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.datax.IDataxReaderContext;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.hdfs.impl.HdfsFileSystemFactory;
import com.qlangtech.tis.offline.FileSystemFactory;
import com.qlangtech.tis.offline.flattable.HiveFlatTableBuilder;
import com.qlangtech.tis.plugin.KeyedPluginStore;
import com.qlangtech.tis.plugin.ValidatorCommons;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.common.PluginFieldValidators;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import org.apache.commons.lang.StringUtils;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

/**
 * @author: baisui 百岁
 * @create: 2021-04-07 15:30
 * @see com.qlangtech.tis.plugin.datax.TisDataXHdfsReader
 **/
public class DataXHdfsReader extends DataxReader implements KeyedPluginStore.IPluginKeyAware {
    // private static final String DATAX_NAME = "Hdfs";

    @FormField(ordinal = 0, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String fsName;

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String path;
    //    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
//    public String defaultFS;
    @FormField(ordinal = 2, type = FormFieldType.ENUM, validate = {Validator.require})
    public String fileType;
    @FormField(ordinal = 3, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String column;

    @FormField(ordinal = 4, type = FormFieldType.INPUTTEXT, validate = {})
    public String fieldDelimiter;

    @FormField(ordinal = 5, type = FormFieldType.ENUM, validate = {})
    public String encoding;

    @FormField(ordinal = 6, type = FormFieldType.INPUTTEXT, validate = {})
    public String nullFormat;

//    @FormField(ordinal = 7, type = FormFieldType.INPUTTEXT, validate = {})
//    public String haveKerberos;
//
//    @FormField(ordinal = 8, type = FormFieldType.INPUTTEXT, validate = {})
//    public String kerberosKeytabFilePath;
//
//    @FormField(ordinal = 9, type = FormFieldType.INPUTTEXT, validate = {})
//    public String kerberosPrincipal;

    @FormField(ordinal = 10, type = FormFieldType.ENUM, validate = {})
    public String compress;

//    @FormField(ordinal = 11, type = FormFieldType.INPUTTEXT, validate = {})
//    public String hadoopConfig;

    @FormField(ordinal = 12, type = FormFieldType.TEXTAREA, validate = {})
    public String csvReaderConfig;

    @FormField(ordinal = 13, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String template;

    public String dataXName;

    @Override
    public void setKey(KeyedPluginStore.Key key) {
        this.dataXName = key.keyVal.getVal();
    }

    private HdfsFileSystemFactory fileSystem;

    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataXHdfsReader.class, "DataXHdfsReader-tpl.json");
    }

    @Override
    public boolean hasMulitTable() {
        return false;
    }


    @Override
    public List<ParseColsResult.DataXReaderTabMeta> getSelectedTabs() {
        DefaultContext context = new DefaultContext();
        ParseColsResult parseOSSColsResult = ParseColsResult.parseColsCfg(new MockFieldErrorHandler(), context, StringUtils.EMPTY, this.column);
        if (!parseOSSColsResult.success) {
            throw new IllegalStateException("parseOSSColsResult must be success");
        }
        return Collections.singletonList(parseOSSColsResult.tabMeta);

    }

    private static class MockFieldErrorHandler implements IFieldErrorHandler {
        @Override
        public void addFieldError(Context context, String fieldName, String msg, Object... params) {
        }

        @Override
        public boolean validateBizLogic(BizLogic logicType, Context context, String fieldName, String value) {
            return false;
        }
    }

    @Override
    public Iterator<IDataxReaderContext> getSubTasks() {
        IDataxReaderContext readerContext = new HdfsReaderContext(this);
        return Collections.singleton(readerContext).iterator();
    }

    public HdfsFileSystemFactory getFs() {
        if (fileSystem == null) {
            this.fileSystem = (HdfsFileSystemFactory) FileSystemFactory.getFsFactory(fsName);
        }
        Objects.requireNonNull(this.fileSystem, "fileSystem has not be initialized");
        return fileSystem;
    }


    @Override
    public String getTemplate() {
        return template;
    }

//    @Override
//    public List<String> getTablesInDB() {
//        throw new UnsupportedOperationException();
//    }


    @TISExtension()
    public static class DefaultDescriptor extends BaseDataxReaderDescriptor {
        private static final Pattern PATTERN_HDFS_RELATIVE_PATH = Pattern.compile("([\\w\\d\\.\\-_=]+/)*([\\w\\d\\.\\-_=]+|(\\*))");

        public DefaultDescriptor() {
            super();
            this.registerSelectOptions(HiveFlatTableBuilder.KEY_FIELD_NAME_FS_NAME
                    , () -> TIS.getPluginStore(FileSystemFactory.class)
                            .getPlugins().stream().filter(((f) -> f instanceof HdfsFileSystemFactory)).collect(Collectors.toList()));
        }

        @Override
        public boolean isRdbms() {
            return false;
        }

        public boolean validateColumn(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            return ParseColsResult.parseColsCfg(msgHandler, context, fieldName, value).success;
        }

        public boolean validatePath(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {

            Matcher matcher = PATTERN_HDFS_RELATIVE_PATH.matcher(value);
            if (!matcher.matches()) {
                msgHandler.addFieldError(context, fieldName, ValidatorCommons.MSG_RELATIVE_PATH_ERROR + ":" + PATTERN_HDFS_RELATIVE_PATH);
                return false;
            }

            return true;
        }


        public boolean validateCsvReaderConfig(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            return PluginFieldValidators.validateCsvReaderConfig(msgHandler, context, fieldName, value);
        }

        @Override
        public String getDisplayName() {
            return DataXHdfsWriter.DATAX_NAME;
        }
    }
}
