///**
// * Licensed to the Apache Software Foundation (ASF) under one
// * or more contributor license agreements.  See the NOTICE file
// * distributed with this work for additional information
// * regarding copyright ownership.  The ASF licenses this file
// * to you under the Apache License, Version 2.0 (the
// * "License"); you may not use this file except in compliance
// * with the License.  You may obtain a copy of the License at
// * <p>
// * http://www.apache.org/licenses/LICENSE-2.0
// * <p>
// * Unless required by applicable law or agreed to in writing, software
// * distributed under the License is distributed on an "AS IS" BASIS,
// * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// * See the License for the specific language governing permissions and
// * limitations under the License.
// */
//
//package com.qlangtech.tis.plugin.datax.kingbase;
//
//import com.qlangtech.tis.datax.IDataxProcessor.TableMap;
//import com.qlangtech.tis.datax.SourceColMetaGetter;
//import com.qlangtech.tis.datax.impl.DataxWriter;
//import com.qlangtech.tis.extension.TISExtension;
//import com.qlangtech.tis.plugin.IEndTypeGetter.EndType;
//import com.qlangtech.tis.plugin.datax.CreateTableSqlBuilder;
//import com.qlangtech.tis.plugin.datax.CreateTableSqlBuilder.ColWrapper;
//import com.qlangtech.tis.plugin.datax.common.impl.ParamsAutoCreateTable;
//import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
//import com.qlangtech.tis.plugin.ds.kingbase.KingBaseDataSourceFactory;
//
//import java.util.Optional;
//
///**
// * @author: 百岁（baisui@qlangtech.com）
// * @create: 2025-01-14 16:01
// **/
//public class KingBaseAutoCreateTable extends ParamsAutoCreateTable<ColWrapper> {
//    @Override
//    public CreateTableSqlBuilder<ColWrapper> createSQLDDLBuilder(
//            DataxWriter rdbmsWriter, SourceColMetaGetter sourceColMetaGetter
//            , TableMap tableMapper, Optional<RecordTransformerRules> transformers) {
//        DataXKingBaseWriter kingBaseWriter = (DataXKingBaseWriter) rdbmsWriter;
//        KingBaseDataSourceFactory dataSourceFactory = kingBaseWriter.getDataSourceFactory();
//
//      return   dataSourceFactory.dbMode.createSQLDDLBuilder(rdbmsWriter,sourceColMetaGetter,tableMapper,transformers);
//
//     //   throw new UnsupportedOperationException();
//    }
//
//    @TISExtension
//    public static class DftDesc extends ParamsAutoCreateTable.DftDesc {
//        @Override
//        public EndType getEndType() {
//            return EndType.KingBase;
//        }
//    }
//}
