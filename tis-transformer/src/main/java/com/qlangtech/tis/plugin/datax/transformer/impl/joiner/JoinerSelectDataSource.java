package com.qlangtech.tis.plugin.datax.transformer.impl.joiner;

import com.alibaba.fastjson.annotation.JSONField;
import com.qlangtech.tis.extension.OneStepOfMultiSteps;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.TableNotFoundException;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;

import java.io.Serializable;
import java.util.List;
import java.util.Optional;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/1/13
 */
public class JoinerSelectDataSource extends OneStepOfMultiSteps implements Serializable {
    @FormField(ordinal = 0, type = FormFieldType.ENUM, validate = {Validator.require})
    public String dbName;

    @JSONField(serialize = false)
    public DataSourceFactory getDataSourceFactory() {
        return DataSourceFactory.load(this.dbName);
    }

    public List<ColumnMetaData> reflectTabCols(String targetTable) throws TableNotFoundException {
        return getDataSourceFactory().getTableMetadata(false, null, EntityName.parse(targetTable));
    }


    @TISExtension
    public static class Desc extends OneStepOfMultiSteps.BasicDesc {

        @Override
        public String getDisplayName() {
            return "第一步";
        }

        @Override
        public String getStepDescription() {
            return "选择DataSource";
        }

        @Override
        public Step getStep() {
            return Step.Step1;
        }

        @Override
        public Optional<BasicDesc> nextPluginDesc() {
            return Optional.of(new JoinerSelectTable.Desc());
        }
    }
}
