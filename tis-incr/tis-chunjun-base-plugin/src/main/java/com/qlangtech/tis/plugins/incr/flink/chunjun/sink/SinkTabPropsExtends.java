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

package com.qlangtech.tis.plugins.incr.flink.chunjun.sink;

import com.dtstack.chunjun.connector.jdbc.dialect.JdbcDialect;
import com.dtstack.chunjun.connector.jdbc.dialect.SupportUpdateMode;
import com.dtstack.chunjun.sink.WriteMode;
import com.google.common.collect.Sets;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.SuFormProperties;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.datax.SelectedTabExtend;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.incr.TISSinkFactory;
import com.qlangtech.tis.plugins.incr.flink.connector.ChunjunSinkFactory;
import com.qlangtech.tis.plugins.incr.flink.connector.UpdateMode;
import org.apache.commons.collections.CollectionUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-07-18 11:51
 **/
public class SinkTabPropsExtends extends SelectedTabExtend {
    public static final String KEY_UNIQUE_KEY = "uniqueKey";
    @FormField(ordinal = 2, type = FormFieldType.ENUM, validate = {Validator.require})
    public UpdateMode incrMode;

    /**
     * SelectedTab 中已经添加了uniqueKey的选项
     *
     * @param params
     */
    public void setParams(Map<String, Object> params) {
        this.incrMode.set(params);
    }

    public static List<String> getDeftRecordKeys() {
        PrimaryKeys primaryKeys = buildPrimaryKeys();
        return primaryKeys.createPkKeys();
    }

    /**
     * 主键候选字段
     *
     * @return
     */
    public static List<Option> getPrimaryKeys() {
        return buildPrimaryKeys().allCols.stream().map((c) -> c).collect(Collectors.toList());
    }

    private static PrimaryKeys buildPrimaryKeys() {
        AtomicReference<List<ColumnMetaData>> colsRef = new AtomicReference<>(Collections.emptyList());
        List<Option> pkResult = SelectedTab.getContextOpts((cols) -> {
            colsRef.set(Collections.unmodifiableList(cols));
            return cols.stream().filter((c) -> c.isPk());
        });

        return new PrimaryKeys(pkResult, colsRef.get());
    }

    private static class PrimaryKeys {
        final List<Option> pks;
        final List<ColumnMetaData> allCols;

        public PrimaryKeys(List<Option> pks, List<ColumnMetaData> allCols) {
            this.pks = pks;
            this.allCols = Objects.requireNonNull(allCols, "param allCols can not be null");
        }

        public List<String> createPkKeys() {
            return pks.stream()
                    .map((pk) -> String.valueOf(pk.getValue())).collect(Collectors.toList());
        }
    }


    /**
     * 写入支持的三种方式
     *
     * @see com.dtstack.chunjun.connector.jdbc.sink.JdbcOutputFormat 的方法 #prepareTemplates
     */
    private static Set<WriteMode> insertSupportedWriteMode = Sets.newHashSet(WriteMode.INSERT, WriteMode.REPLACE, WriteMode.UPSERT);

    /**
     * 由于每种 JdbcDialect 支持的写入类型是不同的所以需要在在运行时 更新下拉列表需要进行过滤
     *
     * @param descs
     * @return
     * @see JdbcDialect
     * @see SupportUpdateMode
     */
    public static List<? extends Descriptor> filter(List<? extends Descriptor> descs) {
        if (CollectionUtils.isEmpty(descs)) {
            throw new IllegalArgumentException("param descs can not be null");
        }
        SuFormProperties.SuFormGetterContext context = SuFormProperties.subFormGetterProcessThreadLocal.get();
        Objects.requireNonNull(context, "context can not be null");
        if (context.param == null) {
            return descs;
        }
        ChunjunSinkFactory sinkFactory = (ChunjunSinkFactory) TISSinkFactory.getIncrSinKFactory(context.param.getDataXName());
        Set<WriteMode> writeModes = sinkFactory.supportSinkWriteMode();
        return descs.stream().filter((d) -> {
            WriteMode wmode = ((UpdateMode.BasicDescriptor) d).writeMode;
            return writeModes.contains(wmode) && insertSupportedWriteMode.contains(wmode);
        }).collect(Collectors.toList());
    }


    @Override
    public ExtendType getExtendType() {
        return ExtendType.INCR_SINK;
    }

    @TISExtension
    public static class DefaultDescriptor extends BaseDescriptor {

    }
}
