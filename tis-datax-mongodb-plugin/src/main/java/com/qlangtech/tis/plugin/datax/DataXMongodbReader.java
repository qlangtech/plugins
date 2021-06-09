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

import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsReader;
import com.qlangtech.tis.plugin.datax.common.RdbmsReaderContext;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.IDataSourceDumper;

/**
 * @author: baisui 百岁
 * @create: 2021-04-07 15:30
 **/
public class DataXMongodbReader extends BasicDataXRdbmsReader {
    private static final String DATAX_NAME = "MongoDB";
    //    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {})
//    public String address;
//    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {})
//    public String userName;
//    @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT, validate = {})
//    public String userPassword;
//    @FormField(ordinal = 3, type = FormFieldType.INPUTTEXT, validate = {})
//    public String collectionName;
//    @FormField(ordinal = 4, type = FormFieldType.INPUTTEXT, validate = {})
//    public String column;
//    @FormField(ordinal = 5, type = FormFieldType.INPUTTEXT, validate = {})
//    public String name;
    @FormField(ordinal = 6, type = FormFieldType.INPUTTEXT, validate = {})
    public String type;
    @FormField(ordinal = 7, type = FormFieldType.INPUTTEXT, validate = {})
    public Boolean splitter;
//    @FormField(ordinal = 8, type = FormFieldType.INPUTTEXT, validate = {})
//    public String query;

//    @FormField(ordinal = 9, type = FormFieldType.TEXTAREA, validate = {Validator.require})
//    public String template;


    @Override
    protected RdbmsReaderContext createDataXReaderContext(
            String jobName, SelectedTab tab, IDataSourceDumper dumper, DataSourceFactory dsFactory) {

        MongoDBReaderContext readerContext = new MongoDBReaderContext(jobName, tab.getName());
        return readerContext;
    }

    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataXMongodbReader.class, "DataXMongodbReader-tpl.json");
    }


    @TISExtension()
    public static class DefaultDescriptor extends BaseDataxReaderDescriptor {
        public DefaultDescriptor() {
            super();
        }

        @Override
        public boolean isRdbms() {
            return true;
        }

        @Override
        public String getDisplayName() {
            return DATAX_NAME;
        }
    }
}
