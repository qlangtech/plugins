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

package com.qlangtech.tis.plugins.incr.flink.chunjun.script;

import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IStreamTableMeataCreator;
import com.qlangtech.tis.datax.TableAlias;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.IEndTypeGetter.EndType;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;
import com.qlangtech.tis.plugins.incr.flink.connector.ChunjunSinkFactory;
import com.qlangtech.tis.plugins.incr.flink.connector.streamscript.BasicFlinkStreamScriptCreator;
import com.qlangtech.tis.sql.parser.tuple.creator.AdapterStreamTemplateData;
import com.qlangtech.tis.sql.parser.tuple.creator.IStreamIncrGenerateStrategy;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.table.factories.Factory;

import java.util.Iterator;
import java.util.List;
import java.util.ServiceLoader;
import java.util.function.Consumer;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-11-14 12:43
 **/
public class ChunjunSqlType extends ChunjunStreamScriptType {

    public static String getTableSinkTypeName(IEndTypeGetter.EndType endType) {
        return "tis-" + endType.getVal() + "-x";
    }

    @Override
    public boolean preValidate(EndType endType, ChunjunSinkFactory sinkFactory, Consumer<String> errorMsgConsumer) {
        String tableSinkTypeName = getTableSinkTypeName(endType);
        // 校验是否已经定义了对应table DynamicTableSinkFactory
        ServiceLoader<Factory> factoriesLoader = ServiceLoader.load(Factory.class, sinkFactory.getClass().getClassLoader());

        Iterator<Factory> iterator = factoriesLoader.iterator();
        Factory factory = null;
        while (iterator.hasNext()) {
            factory = iterator.next();
            if (StringUtils.equalsIgnoreCase(tableSinkTypeName, factory.factoryIdentifier())) {
                return true;
            }
        }
        errorMsgConsumer.accept("未定义对应的Table Sink:'" + tableSinkTypeName + "'，请联系系统管理员");
        return false;
    }

    @Override
    public BasicFlinkStreamScriptCreator createStreamTableCreator(
            IStreamTableMeataCreator.ISinkStreamMetaCreator sinkStreamMetaCreator) {
        return new SQLStreamScriptCreator(sinkStreamMetaCreator);
    }

    protected static class SQLStreamScriptCreator extends BasicFlinkStreamScriptCreator {


        public SQLStreamScriptCreator(IStreamTableMeataCreator.ISinkStreamMetaCreator sinkStreamMetaGetter) {
            super(sinkStreamMetaGetter);

        }


        @Override
        public IStreamTemplateResource getFlinkStreamGenerateTplResource() {
            return IStreamTemplateResource.createStringContentResource(
                    IOUtils.loadResourceFromClasspath(SQLStreamScriptCreator.class, "flink_chujun_table_handle_scala.vm"));
        }

        @Override
        public IStreamTemplateData decorateMergeData(IStreamTemplateData mergeData) {
            return new ChunjunTemplateData(mergeData, this.sinkStreamMetaGetter);
        }
    }

    public static class ChunjunTemplateData extends AdapterStreamTemplateData {
        private IStreamTableMeataCreator.ISinkStreamMetaCreator sinkStreamMetaGetter;
        private final IEndTypeGetter.EndType endType;

        public ChunjunTemplateData(IStreamIncrGenerateStrategy.IStreamTemplateData data
                , IStreamTableMeataCreator.ISinkStreamMetaCreator sinkStreamMetaGetter) {
            super(data);
            this.sinkStreamMetaGetter = sinkStreamMetaGetter;
            ChunjunSinkFactory sinkFactory = (ChunjunSinkFactory) sinkStreamMetaGetter;
            ChunjunSinkFactory.BasicChunjunSinkDescriptor chunjunSinkDesc
                    = (ChunjunSinkFactory.BasicChunjunSinkDescriptor) sinkFactory.getDescriptor();
            this.endType = chunjunSinkDesc.getChunjunEndType();
        }


        public String getSinkTypeName() {
            return getTableSinkTypeName(endType);
        }

        public String getSourceTable(TableAlias alia) {
            return alia.getTo() + KEY_STREAM_SOURCE_TABLE_SUFFIX;
        }

        public List<IColMetaGetter> getCols(IDataxProcessor.TableMap alia) {
            return sinkStreamMetaGetter.getStreamTableMeta(alia).getColsMeta();
        }

        /**
         * https://github.com/datavane/tis/issues/238
         *
         * @param entity
         * @return
         */
        public String escape(String entity) {
            return "`" + entity + "`";
        }


    }


    @TISExtension
    public static class DefaultDescriptor extends Descriptor<ChunjunStreamScriptType> {
        @Override
        public String getDisplayName() {
            return "SQL";
        }
    }
}
