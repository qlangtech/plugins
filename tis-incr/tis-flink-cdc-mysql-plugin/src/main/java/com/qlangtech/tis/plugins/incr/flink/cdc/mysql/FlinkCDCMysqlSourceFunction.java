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

package com.qlangtech.tis.plugins.incr.flink.cdc.mysql;

import com.google.common.collect.Maps;
import com.qlangtech.plugins.incr.flink.cdc.BiFunction;
import com.qlangtech.plugins.incr.flink.cdc.FlinkCol;
import com.qlangtech.plugins.incr.flink.cdc.ISourceValConvert;
import com.qlangtech.plugins.incr.flink.cdc.SourceChannel;
import com.qlangtech.plugins.incr.flink.cdc.SourceChannel.ReaderSourceCreator;
import com.qlangtech.plugins.incr.flink.cdc.TISDeserializationSchema;
import com.qlangtech.plugins.incr.flink.cdc.valconvert.DateTimeConverter;
import com.qlangtech.tis.async.message.client.consumer.AsyncMsg;
import com.qlangtech.tis.async.message.client.consumer.IConsumerHandle;
import com.qlangtech.tis.async.message.client.consumer.IFlinkColCreator;
import com.qlangtech.tis.async.message.client.consumer.IMQListener;
import com.qlangtech.tis.async.message.client.consumer.MQConsumeException;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.DataXName;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.datax.StoreResourceType;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsReader;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.DBConfig.HostDBs;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.RunningContext;
import com.qlangtech.tis.plugin.ds.TableInDB;
import com.qlangtech.tis.plugin.incr.IncrStreamFactory;
import com.qlangtech.tis.plugins.incr.flink.FlinkColMapper;
import com.qlangtech.tis.plugins.incr.flink.cdc.AbstractRowDataMapper;
import com.qlangtech.tis.realtime.ReaderSource;
import com.qlangtech.tis.realtime.dto.DTOStream;
import com.qlangtech.tis.realtime.transfer.DTO;
import com.qlangtech.tis.util.IPluginContext;
import io.debezium.config.CommonConnectorConfig;
import io.debezium.connector.mysql.MySqlConnectorConfig;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.cdc.connectors.mysql.source.MySqlSource;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.ZoneId;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.function.Function;

/**
 * https://nightlies.apache.org/flink/flink-cdc-docs-release-3.2/docs/connectors/flink-sources/mysql-cdc/
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-09-27 15:17
 * @see JobExecutionResult
 **/
public class FlinkCDCMysqlSourceFunction implements IMQListener<List<ReaderSource>> {

    private final FlinkCDCMySQLSourceFactory sourceFactory;


    public FlinkCDCMysqlSourceFunction(FlinkCDCMySQLSourceFactory sourceFactory) {
        this.sourceFactory = sourceFactory;
    }

//    @Override
//    public IConsumerHandle getConsumerHandle() {
//        return this.sourceFactory.getConsumerHander();
//    }

    public static class MySQLSourceDTOColValProcess implements ISourceValConvert, Serializable {
        final Map<String, FlinkColMapper> tabColsMapper;


        public MySQLSourceDTOColValProcess(Map<String, FlinkColMapper> tabColsMapper) {
            this.tabColsMapper = tabColsMapper;
        }

        @Override
        public Object convert(DTO dto, Field field, Object val) {
            FlinkColMapper colMapper = tabColsMapper.get(dto.getTableName());
            if (colMapper == null) {
                throw new IllegalStateException("tableName:" + dto.getTableName()
                        + " relevant colMapper can not be null, exist cols:"
                        + String.join(",", tabColsMapper.keySet()));
            }
            BiFunction process = colMapper.getSourceDTOColValProcess(field.name());
            if (process == null) {
                // 说明用户在选在表的列时候，没有选择该列，所以就不用处理了
                return null;
            }
            return process.apply(val);
        }
    }

    public static class MySQLCDCTypeVisitor extends AbstractRowDataMapper.DefaultTypeVisitor {
        public MySQLCDCTypeVisitor(IColMetaGetter meta, int colIndex) {
            super(meta, colIndex);
        }

        @Override
        public FlinkCol varcharType(DataType type) {
            FlinkCol flinkCol = super.varcharType(type);
            return flinkCol.setSourceDTOColValProcess(new MySQLStringValueDTOConvert());
        }

        @Override
        public FlinkCol blobType(DataType type) {
            FlinkCol flinkCol = super.blobType(type);
            return flinkCol.setSourceDTOColValProcess(new MySQLBinaryRawValueDTOConvert());
        }
    }

    static class MySQLStringValueDTOConvert extends BiFunction {
        @Override
        public Object apply(Object o) {
//before--->Struct{id=1,tiny_c=124,tiny_un_c=255,small_c=32767,small_un_c=65535,medium_c=8388607,medium_un_c=16777215,int_c=2147483647,int_un_c=4294967295,int11_c=2147483647,big_c=9223372036854775807,big_un_c=18446744073709551615,varchar_c=Hello World,char_c=abc,real_c=123.102,float_c=123.10199737548828,double_c=404.4443,decimal_c=123.4567,numeric_c=346,big_decimal_c=34567892.1,bit1_c=false,tiny1_c=1,boolean_c=1,date_c=2020-07-17,time_c=18:00:22,datetime3_c=2020-07-17 18:00:22,datetime6_c=2020-07-17 18:00:22,timestamp_c=2020-07-17 17:40:22,file_uuid=java.nio.HeapByteBuffer[pos=0 lim=16 cap=16],bit_c=[B@5a62bd7b,text_c=text,tiny_blob_c=java.nio.HeapByteBuffer[pos=0 lim=1 cap=1],blob_c=java.nio.HeapByteBuffer[pos=0 lim=1 cap=1],medium_blob_c=java.nio.HeapByteBuffer[pos=0 lim=1 cap=1],long_blob_c=java.nio.HeapByteBuffer[pos=0 lim=1 cap=1],year_c=2021,enum_c=red,set_c=a,b,json_c={"key1":"value1"},point_c=Struct{x=1.0,y=1.0,wkb=[B@5cd2a615}
//,geometry_c=Struct{wkb=[B@68768d6f}
//,linestring_c=Struct{wkb=[B@62a1f465}
//,polygon_c=Struct{wkb=[B@2de7042f},multipoint_c=Struct{wkb=[B@644ced88},multiline_c=Struct{wkb=[B@380d099b},multipolygon_c=Struct{wkb=[B@55e2d023},geometrycollection_c=Struct{wkb=[B@403170cc}}
            /**
             * 测试中发现full_types表中的部分binlog接收到的值是Struct"Struct{wkb=[B@644ced88}" 需要继续拆包才能在下游中使用
             */
            if (o instanceof Struct) {
                Struct val = (Struct) o;
                Schema schema = val.schema();
                StringBuffer vals = new StringBuffer();
                for (Field f : schema.fields()) {
                    vals.append(f.name()).append(":").append(val.get(f)).append(",");
                }
                return vals.toString();
            }
            return o;
        }
    }

    static class MySQLBinaryRawValueDTOConvert extends AbstractRowDataMapper.BinaryRawValueDTOConvert {
        @Override
        public Object apply(Object o) {
//before--->Struct{id=1,tiny_c=124,tiny_un_c=255,small_c=32767,small_un_c=65535,medium_c=8388607,medium_un_c=16777215,int_c=2147483647,int_un_c=4294967295,int11_c=2147483647,big_c=9223372036854775807,big_un_c=18446744073709551615,varchar_c=Hello World,char_c=abc,real_c=123.102,float_c=123.10199737548828,double_c=404.4443,decimal_c=123.4567,numeric_c=346,big_decimal_c=34567892.1,bit1_c=false,tiny1_c=1,boolean_c=1,date_c=2020-07-17,time_c=18:00:22,datetime3_c=2020-07-17 18:00:22,datetime6_c=2020-07-17 18:00:22,timestamp_c=2020-07-17 17:40:22,file_uuid=java.nio.HeapByteBuffer[pos=0 lim=16 cap=16],bit_c=[B@5a62bd7b,text_c=text,tiny_blob_c=java.nio.HeapByteBuffer[pos=0 lim=1 cap=1],blob_c=java.nio.HeapByteBuffer[pos=0 lim=1 cap=1],medium_blob_c=java.nio.HeapByteBuffer[pos=0 lim=1 cap=1],long_blob_c=java.nio.HeapByteBuffer[pos=0 lim=1 cap=1],year_c=2021,enum_c=red,set_c=a,b,json_c={"key1":"value1"},point_c=Struct{x=1.0,y=1.0,wkb=[B@5cd2a615}
//,geometry_c=Struct{wkb=[B@68768d6f}
//,linestring_c=Struct{wkb=[B@62a1f465}
//,polygon_c=Struct{wkb=[B@2de7042f},multipoint_c=Struct{wkb=[B@644ced88},multiline_c=Struct{wkb=[B@380d099b},multipolygon_c=Struct{wkb=[B@55e2d023},geometrycollection_c=Struct{wkb=[B@403170cc}}
            /**
             * 测试中发现full_types表中的部分binlog接收到的值是Struct"Struct{wkb=[B@644ced88}" 需要继续拆包才能在下游中使用
             */
            if (o instanceof Struct) {
                return java.nio.ByteBuffer.wrap((byte[]) ((Struct) o).get("wkb"));
            }
            return super.apply(o);
        }
    }


    /**
     * @param dataxName
     * @param dataSource
     * @param tabs
     * @param dataXProcessor
     * @return
     * @throws MQConsumeException
     * @see JobExecutionResult
     */
    @Override
    public AsyncMsg<List<ReaderSource>> start(IncrStreamFactory streamFactory, boolean flinkCDCPipelineEnable, DataXName dataxName, IDataxReader dataSource
            , List<ISelectedTab> tabs, IDataxProcessor dataXProcessor) throws MQConsumeException {
        try {
            Objects.requireNonNull(dataXProcessor, "param dataXProcessor can not be null");
            BasicDataXRdbmsReader rdbmsReader = (BasicDataXRdbmsReader) dataSource;
            BasicDataSourceFactory dsFactory = (BasicDataSourceFactory) rdbmsReader.getDataSourceFactory();
            Map<String, FlinkColMapper> tabColsMapper = Maps.newHashMap();
            TableInDB tablesInDB = dsFactory.getTablesInDB();
            IFlinkColCreator<FlinkCol> flinkColCreator = sourceFactory.createFlinkColCreator(rdbmsReader);
            IPluginContext pluginContext = IPluginContext.namedContext(dataxName.getPipelineName());
            for (ISelectedTab tab : tabs) {
                FlinkColMapper colsMapper
                        = AbstractRowDataMapper.getAllTabColsMetaMapper(tab.getCols(), flinkColCreator);
                tabColsMapper.put(tab.getName(), colsMapper);
            }
//            DataXName dataXName = pluginContext.getCollectionName();
            Map<String, Map<String, Function<RunningContext, Object>>> contextParamValsGetterMapper
                    = RecordTransformerRules.contextParamValsGetterMapper(
                    dataXProcessor, pluginContext, rdbmsReader, tabs);
            //
            TISDeserializationSchema deserializationSchema
                    = new TISDeserializationSchema(
                    new MySQLSourceDTOColValProcess(tabColsMapper)
                    , tablesInDB.getPhysicsTabName2LogicNameConvertor()
                    , contextParamValsGetterMapper);


            SourceChannel sourceChannel = new SourceChannel(
                    flinkCDCPipelineEnable,
                    SourceChannel.getSourceFunction(
                            dsFactory,
                            tabs
                            , new MySQLReaderSourceCreator(dataxName, streamFactory, flinkCDCPipelineEnable, dsFactory, this.sourceFactory, deserializationSchema)
                    ));
            sourceChannel.setFocusTabs(tabs, dataXProcessor.getTabAlias(null)
                    , (tabName) -> DTOStream.createDispatched(tabName, sourceFactory.independentBinLogMonitor));
            return sourceChannel;
            // return (JobExecutionResult) getConsumerHandle().consume(dataxName, sourceChannel, dataXProcessor);
        } catch (Exception e) {
            throw new MQConsumeException(e.getMessage(), e);
        }
    }

    public static class MySQLReaderSourceCreator implements ReaderSourceCreator {
        private final BasicDataSourceFactory dsFactory;
        private final FlinkCDCMySQLSourceFactory sourceFactory;
        private final TISDeserializationSchema deserializationSchema;
        private static final Logger logger = LoggerFactory.getLogger(MySQLReaderSourceCreator.class);
        private final boolean flinkCDCPipelineEnable;
        private final DataXName dataXName;
        private final IncrStreamFactory streamFactory;

        public MySQLReaderSourceCreator(DataXName dataXName, IncrStreamFactory streamFactory, BasicDataSourceFactory dsFactory, FlinkCDCMySQLSourceFactory sourceFactory) {
            this(dataXName, streamFactory, false, dsFactory, sourceFactory, new TISDeserializationSchema());
        }

        public MySQLReaderSourceCreator(DataXName dataXName, IncrStreamFactory streamFactory, boolean flinkCDCPipelineEnable, BasicDataSourceFactory dsFactory
                , FlinkCDCMySQLSourceFactory sourceFactory, TISDeserializationSchema deserializationSchema) {
            this.dsFactory = dsFactory;
            this.dataXName = Objects.requireNonNull(dataXName, "dataXName can not be null");
            this.sourceFactory = sourceFactory;
            this.deserializationSchema = deserializationSchema;
            this.flinkCDCPipelineEnable = flinkCDCPipelineEnable;
            this.streamFactory = (streamFactory);
        }

        /**
         * @param dbHost
         * @param dbs
         * @param tbs
         * @param debeziumProperties
         * @return
         * @see org.apache.flink.cdc.connectors.mysql.source.connection.PooledDataSourceFactory jdbcConnection create
         */
        @Override
        public List<ReaderSource> create(String dbHost, HostDBs dbs, Set<String> tbs, Properties debeziumProperties) {

            DateTimeConverter.setDatetimeConverters(MySqlDateTimeConverter.class.getName(), debeziumProperties);

            debeziumProperties.setProperty(
                    CommonConnectorConfig.EVENT_PROCESSING_FAILURE_HANDLING_MODE.name()
                    , CommonConnectorConfig.EventProcessingFailureHandlingMode.WARN.getValue());
            // MySqlConnectorConfig.JDBC_DRIVER
            debeziumProperties.setProperty(
                    MySqlConnectorConfig.INCONSISTENT_SCHEMA_HANDLING_MODE.name()
                    , CommonConnectorConfig.EventProcessingFailureHandlingMode.WARN.getValue());

            String[] databases = dbs.getDataBases();
            Optional<ZoneId> timeZone = dsFactory.getTimeZone();
            if (!timeZone.isPresent()) {
                throw new IllegalStateException("timezone can not be null");
            }

            logger.info("monitor db:{} databaseList:{},tableList:{}", dbHost, databases, tbs);
            MySqlSource<DTO> sourceFunc = MySqlSource.<DTO>builder()
                    .hostname(dbHost)
                    .serverTimeZone(timeZone.get().getId())
                    .port(dsFactory.port)
                    .databaseList(databases) // monitor all tables under inventory database
                    .tableList(tbs.toArray(new String[tbs.size()]))
                    // .serverTimeZone(BasicDataSourceFactory.DEFAULT_SERVER_TIME_ZONE.getId())
                    .username(dsFactory.getUserName())
                    .password(dsFactory.getPassword())
                    .startupOptions(sourceFactory.getStartupOptions())
                    .debeziumProperties(debeziumProperties)
                    .deserializer(deserializationSchema) // converts SourceRecord to JSON String
                    .build();

            return createReaderSources(dbHost, dbs, sourceFunc);
        }

        protected List<ReaderSource> createReaderSources(String dbHost, HostDBs dbs, MySqlSource<DTO> sourceFunc) {

            return Collections.singletonList(ReaderSource.createDTOSource(this.streamFactory, this.dataXName,
                            dbHost + ":" + dsFactory.port + ":" + dbs.joinDataBases("_")
                            , flinkCDCPipelineEnable
                            , sourceFunc
                    )
            );
        }
    }
}
