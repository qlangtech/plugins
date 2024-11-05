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

package com.qlangtech.tis.plugin.datax;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.plugin.datax.mongo.MongoWriterSelectedTab;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.DataType.DefaultTypeVisitor;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.trigger.util.JsonUtil;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-20 14:01
 **/
public class MongoDBWriterContext extends BasicMongoDBContext implements IDataxContext {
    private static final Logger logger = LoggerFactory.getLogger(MongoDBWriterContext.class);
    private final DataXMongodbWriter writer;
    private final IDataxProcessor.TableMap tableMapMapper;
    private final MongoWriterSelectedTab selectedTab;
    private final Optional<RecordTransformerRules> transformerRules;

    public MongoDBWriterContext(DataXMongodbWriter writer, IDataxProcessor.TableMap tableMapMapper, Optional<RecordTransformerRules> transformerRules) {
        super(writer.getDsFactory());
        this.tableMapMapper = tableMapMapper;
        this.writer = writer;
        this.selectedTab = (MongoWriterSelectedTab) tableMapMapper.getSourceTab();
        this.transformerRules = transformerRules;
    }

    private final static DefaultTypeVisitor<String> fixType = new DefaultTypeVisitor<String>() {
        @Override
        public String intType(DataType type) {
            return DataXMongodbReader.TYPE_INT;
        }

        @Override
        public String tinyIntType(DataType dataType) {
            return this.intType(dataType);
        }

        @Override
        public String smallIntType(DataType dataType) {
            return this.intType(dataType);
        }

        @Override
        public String bitType(DataType type) {
            return this.intType(type);
        }
    };

    /**
     * 取得默认的列内容
     *
     * @return
     */
    private static String getDftColumn(ISelectedTab tab, Optional<RecordTransformerRules> transformerRules) {
        //[{"name":"user_id","type":"string"},{"name":"user_name","type":"array","splitter":","}]

        JSONArray fields = new JSONArray();

        //        DataxReader dataReader = DataxReader.getThreadBingDataXReader();
        //        if (dataReader == null) {
        //            return "[]";
        //        }
        //
        try {
            List<IColMetaGetter> cols = null;
            if (transformerRules.isPresent()) {
                cols = transformerRules.get().overwriteCols(tab.getCols()).getColsWithoutVirtualInfo();
            } else {
                cols = tab.getCols().stream().collect(Collectors.toList());
            }

            cols.forEach((col) -> {
                JSONObject field = new JSONObject();
                field.put("name", col.getName());
                String finalType = col.getType().accept(fixType);
                field.put("type", StringUtils.defaultIfBlank(finalType, col.getType().getCollapse().getLiteria()));
                fields.add(field);
            });


        } catch (Exception e) {
            logger.warn(e.getMessage(), e);
            return "[]";
        }

        return JsonUtil.toString(fields);
    }

    public String getDataXName() {
        return this.writer.dataXName;
    }

    public String getCollectionName() {
        return tableMapMapper.getTo(); //this.writer.collectionName;
    }

    public String getColumn() {
        return getDftColumn(tableMapMapper.getSourceTab(), this.transformerRules);
    }


    public boolean isContainUpsertInfo() {
        return this.selectedTab.upsert.supportUpset();
        // return StringUtils.isNotBlank(this.writer.upsertInfo);
    }

    public String getUpsertInfo() {
        return JsonUtil.toString(this.selectedTab.upsert.getUpsetCfg());
    }
}
