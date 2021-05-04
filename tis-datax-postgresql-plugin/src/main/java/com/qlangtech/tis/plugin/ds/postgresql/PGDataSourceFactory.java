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

package com.qlangtech.tis.plugin.ds.postgresql;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.DataDumpers;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.IDataSourceDumper;
import com.qlangtech.tis.plugin.ds.TISTable;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.util.IPluginContext;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-04-23 19:03
 **/
public class PGDataSourceFactory extends DataSourceFactory {
    public static final String DS_TYPE_PG = "PG";

    // 数据库名称
    @FormField(identity = true, ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
    public String name;

    @FormField(ordinal = 3, type = FormFieldType.INPUTTEXT, validate = {Validator.require})
    public String jdbcURL;

    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
    public String userName;

    @FormField(ordinal = 2, type = FormFieldType.PASSWORD, validate = {})
    public String password;


    @Override
    public DataDumpers getDataDumpers(TISTable table) {
        Iterator<IDataSourceDumper> dumpers = null;
        return new DataDumpers(1, dumpers);
    }

    @TISExtension
    public static class DefaultDescriptor extends DataSourceFactory.BaseDataSourceFactoryDescriptor {
        @Override
        protected String getDataSourceName() {
            return DS_TYPE_PG;
        }

        @Override
        public boolean supportFacade() {
            return false;
        }

        @Override
        public List<String> facadeSourceTypes() {
            return Collections.emptyList();
        }

        @Override
        protected boolean validate(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {

            ParseDescribable<DataSourceFactory> pgDataSource = this.newInstance((IPluginContext) msgHandler, postFormVals.rawFormData, Optional.empty());

            try {
                List<String> tables = pgDataSource.instance.getTablesInDB();
                msgHandler.addActionMessage(context, "find " + tables.size() + " table in db");
            } catch (Exception e) {
                msgHandler.addErrorMessage(context, e.getMessage());
                return false;
            }

            return true;
        }
    }

}
