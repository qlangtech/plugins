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

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.manage.common.PropertyPlaceholderHelper;
import com.qlangtech.tis.plugin.KeyedPluginStore;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.DataSourceFactoryPluginStore;
import com.qlangtech.tis.plugin.ds.PostedDSProp;
import com.qlangtech.tis.plugin.ds.clickhouse.ClickHouseDataSourceFactory;
import org.apache.commons.lang.StringUtils;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * https://github.com/alibaba/DataX/blob/master/clickhousewriter/src/main/resources/plugin_job_template.json
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-16 21:48
 **/
public class DataXClickhouseWriter extends DataxWriter implements KeyedPluginStore.IPluginKeyAware {

    public static final String DATAX_NAME = "ClickHouse";

    @FormField(ordinal = 0, type = FormFieldType.ENUM, validate = {Validator.require})
    public String dbName;

    @FormField(ordinal = 5, type = FormFieldType.INPUTTEXT, validate = {})
    public String preSql;
    @FormField(ordinal = 6, type = FormFieldType.INPUTTEXT, validate = {})
    public String postSql;
    @FormField(ordinal = 7, type = FormFieldType.INT_NUMBER, validate = {Validator.require})
    public Integer batchSize;

    @FormField(ordinal = 8, type = FormFieldType.INT_NUMBER, validate = {Validator.require})
    public Integer batchByteSize;

    @FormField(ordinal = 9, type = FormFieldType.ENUM, validate = {Validator.require})
    public String writeMode;

//    @FormField(ordinal = 10, type = FormFieldType.ENUM, validate = {Validator.require})
//    public Boolean autoCreateTable;


    @FormField(ordinal = 79, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String template;

    public String dataXName;

    @Override
    public void setKey(KeyedPluginStore.Key key) {
        this.dataXName = key.keyVal.getVal();
    }

    @Override
    public IDataxContext getSubTask(Optional<IDataxProcessor.TableMap> tableMap) {
        if (!tableMap.isPresent()) {
            throw new IllegalArgumentException("tableMap shall be present");
        }
        IDataxProcessor.TableMap tmapper = tableMap.get();
        ClickHouseDataSourceFactory ds = getDataSourceFactory();

        ClickHouseWriterContext context = new ClickHouseWriterContext();
        context.setBatchByteSize(this.batchByteSize);
        context.setBatchSize(this.batchSize);
        context.setJdbcUrl(ds.jdbcUrl);
        context.setUsername(ds.username);
        context.setPassword(ds.password);
        context.setTable(tmapper.getTo());
        context.setWriteMode(this.writeMode);
        PropertyPlaceholderHelper helper = new PropertyPlaceholderHelper("${", "}");
        Map<String, String> params = new HashMap<>();
        params.put("table", tmapper.getTo());
        PropertyPlaceholderHelper.PlaceholderResolver resolver = (key) -> {
            return params.get(key);
        };
        if (StringUtils.isNotEmpty(this.preSql)) {
            context.setPreSql(helper.replacePlaceholders(this.preSql, resolver));
        }

        if (StringUtils.isNotEmpty(this.postSql)) {
            context.setPostSql(helper.replacePlaceholders(this.postSql, resolver));
        }
        context.setCols(IDataxProcessor.TabCols.create(tableMap.get()));
        return context;
    }

    @Override
    public String getTemplate() {
        return template;
    }


    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(
                DataXClickhouseWriter.class, "DataXClickhouseWriter-tpl.json");
    }

    public ClickHouseDataSourceFactory getDataSourceFactory() {
        DataSourceFactoryPluginStore dsStore = TIS.getDataBasePluginStore(new PostedDSProp(this.dbName));
        return dsStore.getDataSource();
    }


    @TISExtension()
    public static class DefaultDescriptor extends DataxWriter.BaseDataxWriterDescriptor {
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
