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

import com.qlangtech.tis.datax.IDataxReaderContext;
import com.qlangtech.tis.datax.ISelectedTab;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;

import java.util.Iterator;
import java.util.List;

/**
 * @author: baisui 百岁
 * @create: 2021-04-07 15:30
 **/
public class DataXHdfsReader extends DataxReader {
    private static final String DATAX_NAME = "Hdfs";
    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String path;
    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String defaultFS;
    @FormField(ordinal = 2, type = FormFieldType.ENUM, validate = {Validator.require})
    public String fileType;
    @FormField(ordinal = 3, type = FormFieldType.INPUTTEXT, validate = {})
    public String column;

    @FormField(ordinal = 4, type = FormFieldType.INPUTTEXT, validate = {})
    public String fieldDelimiter;

    @FormField(ordinal = 5, type = FormFieldType.INPUTTEXT, validate = {})
    public String encoding;

    @FormField(ordinal = 6, type = FormFieldType.INPUTTEXT, validate = {})
    public String nullFormat;

    @FormField(ordinal = 7, type = FormFieldType.INPUTTEXT, validate = {})
    public String haveKerberos;

    @FormField(ordinal = 8, type = FormFieldType.INPUTTEXT, validate = {})
    public String kerberosKeytabFilePath;

    @FormField(ordinal = 9, type = FormFieldType.INPUTTEXT, validate = {})
    public String kerberosPrincipal;

    @FormField(ordinal = 10, type = FormFieldType.INPUTTEXT, validate = {})
    public String compress;

    @FormField(ordinal = 11, type = FormFieldType.INPUTTEXT, validate = {})
    public String hadoopConfig;

    @FormField(ordinal = 12, type = FormFieldType.INPUTTEXT, validate = {})
    public String csvReaderConfig;

    @FormField(ordinal = 13, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String template;

    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataXHdfsReader.class, "DataXHdfsReader-tpl.json");
    }

    @Override
    public boolean hasMulitTable() {
        return false;
    }

    @Override
    public <T extends ISelectedTab> List<T> getSelectedTabs() {
        return null;
    }

    @Override
    public boolean hasExplicitTable() {
        return false;
    }

    @Override
    public Iterator<IDataxReaderContext> getSubTasks() {

        return null;
    }


    @Override
    public String getTemplate() {
        return template;
    }

    // public void setSelectedTabs(List<SelectedTab> selectedTabs) {
    //     this.selectedTabs = selectedTabs;
    // }

    @Override
    public List<String> getTablesInDB() {
//        DataSourceFactory plugin = getDataSourceFactory();
//        return plugin.getTablesInDB();
        return null;
    }


    @Override
    public List<ColumnMetaData> getTableMetadata(String table) {
        return null;
    }

    @TISExtension()
    public static class DefaultDescriptor extends Descriptor<DataxReader> {
        public DefaultDescriptor() {
            super();
        }

        @Override
        public String getDisplayName() {
            return DATAX_NAME;
        }
    }
}
