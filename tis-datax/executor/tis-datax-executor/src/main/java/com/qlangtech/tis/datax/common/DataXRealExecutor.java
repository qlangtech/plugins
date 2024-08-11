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

package com.qlangtech.tis.datax.common;

import com.alibaba.datax.common.element.ColumnCast;
import com.alibaba.datax.common.element.DataXResultPreviewOrderByCols;
import com.alibaba.datax.common.element.DataXResultPreviewOrderByCols.OffsetColVal;
import com.alibaba.datax.common.element.PreviewRecords;
import com.alibaba.datax.common.element.QueryCriteria;
import com.alibaba.datax.common.element.ThreadLocalRows;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.job.JobContainer;
import com.alibaba.datax.core.util.container.CoreConstant;
import com.alibaba.datax.core.util.container.LoadUtil;
import com.qlangtech.tis.datax.DataxExecutor;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxProcessor.TableMap;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.datax.IDataxReaderContext;
import com.qlangtech.tis.datax.IGroupChildTaskIterator;
import com.qlangtech.tis.datax.TimeFormat;
import com.qlangtech.tis.datax.impl.DataXCfgGenerator;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.StoreResourceType;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Consumer;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-07-23 17:52
 **/
public class DataXRealExecutor {

    private final String dataXName;
    private final IDataxReader dataxReader;
    private final WriterPluginMeta writerPluginMeta;
    public final DataXCfgGenerator dataXCfgGenerator;
    public final IDataxProcessor dataxProcessor;

    public DataXRealExecutor(final String dataXName
            , IDataxProcessor dataxProcessor
            , IDataxReader dataxReader
            , WriterPluginMeta writerPluginMeta, DataXCfgGenerator dataXCfgGenerator) {
        this.dataXName = dataXName;
        this.dataxReader = dataxReader;
        this.writerPluginMeta = writerPluginMeta;
        this.dataXCfgGenerator = dataXCfgGenerator;
        this.dataxProcessor = dataxProcessor;

    }

    private IDataxReader getDataxReader() {
        return this.dataxReader;
    }


    private boolean isNumericJdbcType(Map<String, DataType> typeMap, String colKey) {
        switch (Objects.requireNonNull(typeMap.get(colKey)
                , "colKey:" + colKey + " relevant dataType can not be null").getCollapse()) {
            case INT:
            case Long:
            case Double:
            case Boolean:
                return true;
            default:
                return false;
        }
    }


    /**
     * 预览数据 针对表的基于pager的数据分页显示
     *
     * @param tableName
     * @param queryCriteria
     * @return
     */
    public PreviewRecords previewRecords(String tableName, QueryCriteria queryCriteria) {

        if (StringUtils.isEmpty(tableName)) {
            throw new IllegalArgumentException("param tableName can not be null");
        }
        IGroupChildTaskIterator subTasks = this.getDataxReader().getSubTasks((tab) -> StringUtils.equals(tab.getName(), tableName));
        IPluginContext pluginCtx = IPluginContext.namedContext(this.dataXName);
        while (subTasks.hasNext()) {
            IDataxReaderContext readerContext = subTasks.next();

            Optional<IDataxProcessor.TableMap> tableMap = this.dataXCfgGenerator.buildTabMapper(this.getDataxReader(), readerContext);

            RecordTransformerRules transformerRules
                    = RecordTransformerRules.loadTransformerRules(pluginCtx, readerContext.getSourceEntityName());

            Configuration readerCfg
                    = Configuration.from(this.dataXCfgGenerator.generateDataxConfig(readerContext
                    , this.dataxProcessor.getWriter(pluginCtx), this.getDataxReader(), transformerRules, tableMap));

            ThreadLocalRows rows = new ThreadLocalRows();
            ISelectedTab tab = null;
            if (tableMap.isPresent()) {
                TableMap tabMapper = tableMap.get();
                tab = tabMapper.getSourceTab();
                List<String> primaryKeys = tab.getPrimaryKeys();
                List<OffsetColVal> offsetPointer = queryCriteria.getPagerOffsetCursor();
                boolean firstPage = CollectionUtils.isEmpty(offsetPointer);
                DataXResultPreviewOrderByCols orderByCols = new DataXResultPreviewOrderByCols(firstPage);

                if (firstPage) {
                    for (String pk : primaryKeys) {
                        orderByCols.addOffsetColVal(new OffsetColVal(pk, null, null));
                    }
                } else {
//                    Map<String, DataType> typeMap
//                            = tab.getCols().stream().collect(Collectors.toMap((col) -> col.getName(), (col) -> col.getType()));
                    // for (String pk : primaryKeys) {
                    for (OffsetColVal val : offsetPointer) {
                        orderByCols.addOffsetColVal(val);
                    }
                    //   orderByCols.addOffsetColVal(new OffsetColVal(pk, offsetPointer.get(pk), isNumericJdbcType(typeMap, pk)));
                    // }
                }
                rows.setPagerOffsetPointCols(orderByCols);
            }

            rows.setQuery(queryCriteria);

            // TISJarLoader uberClassLoader = new TISJarLoader(TIS.get().getPluginManager(), LocalDataXJobSubmit.class.getClassLoader());

            Optional<Pair<String, List<String>>> transformer = Optional.empty();
            if (transformerRules != null) {
                transformer = Optional.of(Pair.of(tableName, transformerRules.relevantColKeys()));
            }

            this.startPipeline(readerCfg, transformer, (jobContainer) -> {
                jobContainer.setAttr(ThreadLocalRows.class, rows);
            });

            return rows.createPreviewRecords(tab);

            //  return new PreviewRecords( rows.getRows(), );
        }

        throw new IllegalStateException("can not arrive here ,tableName:" + tableName);
    }


    /**
     * 开始执行数据同步
     *
     * @param readerCfg
     * @param transformer
     * @param jobContainerSetter
     */
    public void startPipeline(Configuration readerCfg, Optional<Pair<String, List<String>>> transformer, Consumer<JobContainer> jobContainerSetter) {
        Objects.requireNonNull(readerCfg);

        Configuration allConf = IOUtils.loadResourceFromClasspath(DataxExecutor.class //
                , "core.json", true, (input) -> {
                    Configuration cfg = Configuration.from(input);

                    cfg.set(writerPluginMeta.getPluginKey() + ".class", writerPluginMeta.getStreamwriterClass());

                    cfg.set("plugin.reader." + dataxReader.getDataxMeta().getName() + ".class", dataxReader.getDataxMeta().getImplClass());
                    transformer.ifPresent((tt) -> {
                        Pair<String, List<String>> t = tt;
                        Configuration c = Configuration.newDefault();
                        c.set(CoreConstant.JOB_TRANSFORMER_NAME, t.getKey());
                        c.set(CoreConstant.JOB_TRANSFORMER_RELEVANT_KEYS, t.getRight());
                        cfg.set("job.content[0]." + CoreConstant.JOB_TRANSFORMER, c);
                    });

                    cfg.set("job.content[0].reader", readerCfg);
                    cfg.set("job.content[0].writer", writerPluginMeta.getConf());

                    DataxExecutor.setResType(cfg, StoreResourceType.DataApp);
                    return cfg;
                });


        // 绑定column转换信息
        ColumnCast.bind(allConf);
        LoadUtil.bind(allConf);

        JobContainer container = new JobContainer(allConf) {
            @Override
            public int getTaskSerializeNum() {
                return 999;
            }

            @Override
            protected List<Configuration> mergeReaderAndWriterTaskConfigs(List<Configuration> readerTasksConfigs
                    , List<Configuration> writerTasksConfigs, Optional<Configuration> transformerConfigs) {
                List<Configuration> cfgs = super.mergeReaderAndWriterTaskConfigs(readerTasksConfigs, writerTasksConfigs, transformerConfigs);
                for (Configuration cfg : cfgs) {
                    return Collections.singletonList(cfg);
                }
                throw new IllegalStateException("cfgs shall not be empty");
            }

            @Override
            public String getFormatTime(TimeFormat format) {
                return super.getFormatTime(format);
            }

            public String getDataXName() {
                return dataXName;
            }

            @Override
            public String getTISDataXName() {
                return dataXName;
            }
        };
        jobContainerSetter.accept(container);
        container.start();
    }
}
