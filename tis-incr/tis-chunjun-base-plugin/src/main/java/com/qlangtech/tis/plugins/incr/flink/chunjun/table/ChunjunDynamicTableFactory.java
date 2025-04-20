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

package com.qlangtech.tis.plugins.incr.flink.chunjun.table;

import com.dtstack.chunjun.connector.jdbc.table.JdbcDynamicTableFactory;
import com.google.common.collect.Sets;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.datax.DataXName;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.TableAlias;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.offline.DataxUtils;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.incr.TISSinkFactory;
import com.qlangtech.tis.plugins.incr.flink.chunjun.script.ChunjunSqlType;
import com.qlangtech.tis.plugins.incr.flink.connector.ChunjunSinkFactory;
import com.qlangtech.tis.realtime.BasicTISSinkFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Collections;
import java.util.Set;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-11-16 10:59
 * @see JdbcDynamicTableFactory
 **/
public abstract class ChunjunDynamicTableFactory implements DynamicTableSinkFactory {

    private final IEndTypeGetter.EndType endType;

    public static final ConfigOption<String> dataXNameOpt
            = ConfigOptions.key((DataxUtils.DATAX_NAME)).stringType().noDefaultValue();
    public static final ConfigOption<String> sourceTableNameOpt
            = ConfigOptions.key((TableAlias.KEY_FROM_TABLE_NAME)).stringType().noDefaultValue();

    public ChunjunDynamicTableFactory(IEndTypeGetter.EndType endType) {
        this.endType = endType;
    }


    @Override
    public DynamicTableSink createDynamicTableSink(Context context) {
        final FactoryUtil.TableFactoryHelper helper =
                FactoryUtil.createTableFactoryHelper(this, context);
        // 1.所有的requiredOptions和optionalOptions参数
        final ReadableConfig config = helper.getOptions();

        // 2.参数校验
        helper.validate();
//        TableSchema physicalSchema =
//                TableSchemaUtils.getPhysicalSchema(context.getCatalogTable().getSchema());
        // validateConfigOptions(config, physicalSchema);
//        JdbcDialect jdbcDialect = getDialect();


        String dataXName = config.get(dataXNameOpt); //(config.get(StringUtils.lowerCase(DataxUtils.DATAX_NAME)));
        String sourceTableName = config.get(sourceTableNameOpt);//  //properties.get(StringUtils.lowerCase(TableAlias.KEY_FROM_TABLE_NAME));
        if (StringUtils.isEmpty(dataXName) || StringUtils.isEmpty(sourceTableName)) {
            throw new IllegalArgumentException("param dataXName or sourceTableName can not be null");
        }
        ChunjunSinkFactory sinKFactory = (ChunjunSinkFactory) TISSinkFactory.getIncrSinKFactory(DataXName.createDataXPipeline(dataXName));
        IDataxProcessor dataxProcessor = DataxProcessor.load(null, dataXName);

        BasicTISSinkFactory.RowDataSinkFunc rowDataSinkFunc = sinKFactory.createRowDataSinkFunc(dataxProcessor
                , dataxProcessor.getTabAlias(null).getWithCheckNotNull(sourceTableName), false);

        // 3.封装参数
        return new TISJdbcDymaincTableSink(this.endType, rowDataSinkFunc);
    }

    @Override
    public String factoryIdentifier() {
        return ChunjunSqlType.getTableSinkTypeName(this.endType);
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        return Sets.newHashSet(dataXNameOpt, sourceTableNameOpt);
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        return Collections.emptySet();
    }
}
