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

package com.qlangtech.tis.plugin.datax.kafka.reader;

import com.alibaba.citrus.turbine.Context;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.qlangtech.tis.datax.IDataxProcessor.TableMap;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.datax.IGroupChildTaskIterator;
import com.qlangtech.tis.datax.SourceColMetaGetter;
import com.qlangtech.tis.datax.impl.DataxReader;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.SubFormFilter;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.lang.TisException;
import com.qlangtech.tis.lang.TisException.ErrMsg;
import com.qlangtech.tis.plugin.IPluginStore.AfterPluginSaved;
import com.qlangtech.tis.plugin.KeyedPluginStore;
import com.qlangtech.tis.plugin.KeyedPluginStore.Key;
import com.qlangtech.tis.plugin.ValidatorCommons;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.SubForm;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.datax.format.guesstype.GuessFieldType;
import com.qlangtech.tis.plugin.datax.format.guesstype.StructuredReader;
import com.qlangtech.tis.plugin.datax.format.guesstype.StructuredReader.StructuredRecord;
import com.qlangtech.tis.plugin.datax.kafka.reader.subscriptionmethod.KafkaSubscriptionMethod;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.ContextParamConfig;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.RunningContext;
import com.qlangtech.tis.plugin.ds.TableNotFoundException;
import com.qlangtech.tis.plugin.kafka.consumer.KafkaDTODeserializationSchema;
import com.qlangtech.tis.plugin.kafka.consumer.KafkaStructuredRecord;
import com.qlangtech.tis.plugins.datax.kafka.writer.protocol.KafkaProtocol;
import com.qlangtech.tis.plugins.incr.flink.chunjun.kafka.format.FormatFactory;
import com.qlangtech.tis.plugins.incr.flink.chunjun.kafka.format.FormatFactory.BasicFormatDescriptor;
import com.qlangtech.tis.realtime.transfer.DTO;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.KafkaSourceBuilder;
import org.apache.flink.connector.kafka.source.enumerator.subscriber.KafkaSubscriber;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static com.qlangtech.tis.plugin.annotation.Validator.identity;
import static com.qlangtech.tis.plugin.annotation.Validator.require;
import static com.qlangtech.tis.plugin.ds.ContextParamConfig.KEY_DB_CONTEXT_PARAM_NAME;

/**
 * reference： https://github.com/airbytehq/airbyte/blob/master/airbyte-integrations/connectors/source-kafka/src/main/resources/spec.json
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-03-16 22:25
 **/
public class DataXKafkaReader extends DataxReader implements AfterPluginSaved, KeyedPluginStore.IPluginKeyAware {
    private static final Logger logger = LoggerFactory.getLogger(DataXKafkaReader.class);
    public static final String FIELD_KEY_FORMAT = "format";

    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = {require})
    public String bootstrapServers;

    @FormField(ordinal = 1, validate = {require})
    public KafkaSubscriptionMethod subscription;
    @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT, validate = {}, advance = true)
    public String testTopic;


    @FormField(ordinal = 4, validate = {Validator.require})
    public GuessFieldType guessFieldType;

    @FormField(ordinal = 3, validate = {require})
    public FormatFactory format;

    @FormField(ordinal = 5, type = FormFieldType.INPUTTEXT, validate = {require, identity})
    public String groupId;

    @FormField(ordinal = 6, validate = {require})
    public KafkaProtocol protocol;

    public static List<? extends Descriptor> supportedFormats(List<? extends Descriptor> descs) {
        if (CollectionUtils.isEmpty(descs)) {
            return Collections.emptyList();
        }
        return descs.stream().filter((desc) ->
                ((BasicFormatDescriptor) desc).getEndType().sourceSupport).collect(Collectors.toList());
    }

    @Override
    public final Map<String, ContextParamConfig> getDBContextParams() {
        return ContextParamConfig.defaultContextParams((param) -> !KEY_DB_CONTEXT_PARAM_NAME.equals(param.getKeyName()));
    }

//    @FormField(ordinal = 5, advance = true, type = FormFieldType.INT_NUMBER, validate = {require})
//    public Integer inspectRowCount;


    final KafkaConsumerFactory createKafkaFactory() {
        return KafkaConsumerFactory.getKafkaConfig(this, true);
    }


    @FormField(ordinal = 6, type = FormFieldType.ENUM, validate = {})
    public Boolean enableAutoCommit;


    @FormField(ordinal = 5, type = FormFieldType.ENUM, validate = {})
    public String clientDnsLookup;

    @FormField(ordinal = 4, type = FormFieldType.INT_NUMBER, validate = {Validator.integer})
    public Integer requestTimeoutMs;


    @FormField(ordinal = 7, type = FormFieldType.INPUTTEXT, validate = {})
    public String clientId;


    @FormField(ordinal = 9, type = FormFieldType.INT_NUMBER, validate = {Validator.integer})
    public Integer retryBackoffMs;


    @FormField(ordinal = 12, type = FormFieldType.INT_NUMBER, validate = {Validator.integer}, advance = true)
    public Integer autoCommitIntervalMs;

//    @FormField(ordinal = 13, type = FormFieldType.ENUM, validate = {}, advance = true)
//    public String autoOffsetReset;


    @FormField(ordinal = 16, type = FormFieldType.INT_NUMBER, validate = {Validator.integer}, advance = true)
    public Integer maxPollRecords;

    @FormField(ordinal = 17, type = FormFieldType.INT_NUMBER, validate = {Validator.integer}, advance = true)
    public Integer receiveBufferBytes;

    @FormField(ordinal = 18, type = FormFieldType.INT_NUMBER, validate = {Validator.integer}, advance = true)
    public Integer socketConnectionSetupTimeoutMs;

    @FormField(ordinal = 19, type = FormFieldType.INT_NUMBER, validate = {Validator.integer}, advance = true)
    public Integer socketConnectionSetupTimeoutMaxMs;
    public transient String dataXName;
    /**
     * 增量执行过程需要的配置参数
     */

    @FormField(ordinal = 20, type = FormFieldType.INT_NUMBER, validate = {Validator.integer})
    public Integer maxRecordsProcess;
    @FormField(ordinal = 21, type = FormFieldType.INT_NUMBER, validate = {Validator.integer}, advance = true)
    public Integer repeatedCalls;

    @FormField(ordinal = 22, type = FormFieldType.INT_NUMBER, validate = {Validator.integer}, advance = true)
    public Integer pollingTime;

    @SubForm(desClazz = SelectedTab.class //
            , idListGetScript = "return com.qlangtech.tis.plugin.datax.kafka.reader.DataXKafkaReader.getTablesInDB(filter);", atLeastOne = true)
    public transient List<SelectedTab> selectedTabs;

    private transient Set<String> _targetTabsEntities;
    private transient Map<String /**tabName*/, Map<String /**colName*/, ColumnMetaData>> _tabColsMeta;

    public static List<String> getTablesInDB(SubFormFilter filter) {
        DataXKafkaReader reader = DataxReader.getDataxReader(filter);
        return Lists.newArrayList(reader.getTargetTabsEntities());
    }

    @Override
    public void refresh() {
        super.refresh();
        if (StringUtils.isEmpty(this.dataXName)) {
            throw new IllegalStateException("property dataName can not be empty");
        }
        this.tableColsTypeCache.invalidate(this.dataXName);
    }

    //    public  Map<String, ContextParamConfig> getDBContextParams() {
//        return  ContextParamConfig.defaultContextParams();
//    }
    @Override
    public void setKey(Key key) {
        this.dataXName = key.keyVal.getVal();
    }

    @Override
    public SourceColMetaGetter createSourceColMetaGetter() {
        return new SourceColMetaGetter(this, true) {
            @Override
            protected Map<String, ColumnMetaData> getColMetaDataMap(IDataxReader dataXReader, TableMap tableMapper) {
                // 在生成ddl 时使用的meta信息
                DataXKafkaReader ts = DataXKafkaReader.this;
                final String tabName = tableMapper.getFrom();
                if (ts._tabColsMeta != null) {
                    return ts._tabColsMeta.get(tabName);
                }

                if (CollectionUtils.isNotEmpty(ts.selectedTabs)) {
                    int[] index = new int[1];
                    ts._tabColsMeta = ts.selectedTabs.stream().collect(
                            Collectors.toMap((tab) -> {
                                        index[0] = 0;
                                        return tab.getName();
                                    }
                                    , (tab) -> tab.getCols().stream()
                                            .collect(Collectors.toMap((col) -> col.getName()
                                                    , (col) -> new ColumnMetaData(index[0]++, col.getName(), col.getType(), col.isPk()))
                                            )));
                    return ts._tabColsMeta.get(tabName);
                }

                throw new IllegalStateException("table:" + tabName
                        + " can not find relevant cols meta,selectedTabs isEmpty:"
                        + CollectionUtils.isEmpty(ts.selectedTabs));
            }
        };
    }

    @Override
    public IGroupChildTaskIterator getSubTasks(Predicate<ISelectedTab> filter) {

        Objects.requireNonNull(this.selectedTabs, "selectedTabs can not be null");
        List<SelectedTab> tabs = this.selectedTabs.stream().filter(filter).collect(Collectors.toList());

        return new DataXKafkaGroupChildTaskIterator(this, tabs);
    }

    public Set<String> getTargetTabsEntities() {
        if (this._targetTabsEntities == null) {
            this._targetTabsEntities = Sets.newTreeSet(String.CASE_INSENSITIVE_ORDER);
            this._targetTabsEntities.addAll(this.format.parseTargetTabsEntities());

        }
        return this._targetTabsEntities;
    }

    @Override
    public void afterSaved(IPluginContext pluginContext, Optional<Context> context) {
        this._targetTabsEntities = null;
        this._tabColsMeta = null;
    }

    private static final Cache<String, Map<String/*tableName*/, List<ColumnMetaData>>> tableColsTypeCache
            = CacheBuilder.newBuilder().expireAfterAccess(10, TimeUnit.MINUTES).build();


    @Override
    public List<ColumnMetaData> getTableMetadata(boolean inSink, IPluginContext pluginContext, EntityName table) throws TableNotFoundException {
        //  return super.getTableMetadata(inSink, table);
        return parseTableMetadataFromKafkaHistoryEvents(table);
    }

    private List<ColumnMetaData> parseTableMetadataFromKafkaHistoryEvents(EntityName table) {
        if (StringUtils.isEmpty(this.dataXName)) {
            throw new IllegalStateException("property dataXName can not be null");
        }
        try {

            Map<String/*tableName*/, List<ColumnMetaData>> tabColMeta = tableColsTypeCache.get(this.dataXName, () -> {
                KafkaConsumerFactory kafkaFactory = KafkaConsumerFactory.getKafkaConfig(DataXKafkaReader.this, (props) -> {
                    Map<String, Object> rewrite = Maps.newHashMap(props);
                    // 默认从事件流最前开始读
                    rewrite.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, StringUtils.lowerCase(OffsetResetStrategy.EARLIEST.name()));
                    rewrite.put(ConsumerConfig.GROUP_ID_CONFIG, "kafka-guess-type");
                    rewrite.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, (false));

                    return rewrite;
                }, true);

                KafkaSubscriber topicSubscriber = getTopicSubscriber();
                Set<TopicPartition> topicPartitions = null;
                try (AdminClient adminClient = AdminClient.create(this.buildKafkaProperties())) {
                    topicPartitions = topicSubscriber.getSubscribedTopicPartitions(adminClient);
                }
                if (CollectionUtils.isEmpty(topicPartitions)) {
                    throw new IllegalStateException("topicPartitions can not be empty");
                }

                try (KafkaConsumer<byte[], byte[]> consumer = kafkaFactory.getConsumer()) {
                    //  subscription.setSubscription(consumer);
                    consumer.assign(topicPartitions);
                    // 确保从最先的地方 开始消费
//                    for (TopicPartition partition : topicPartitions) {
//                        consumer.seek(partition, 0);
//                    }
//                    consumer.listTopics()
                    consumer.seekToBeginning(topicPartitions);

                    // reference:https://github.com/airbytehq/airbyte/blob/fefbb72efa10c695c80ed04cbb564ad85aa39705/airbyte-integrations/connectors/source-kafka/src/main/java/io/airbyte/integrations/source/kafka/format/JsonFormat.java#L118C91-L118C103
                    GuessStructuredReader consumerRecords = new GuessStructuredReader(consumer.poll(Duration.of(pollingTime, ChronoUnit.MILLIS)));
                    //                                int readCount = 0;
                    //                                byte[] val = null;
                    //                                Map<String, Object> row = null;
                    // IGuessColTypeFormatConfig formatConfig = null;

                    try {

                        Set<String> targetTabs = getTargetTabsEntities();

                        Map<String/*tableName*/, Map<String/*colName*/, DataType>> colMeta = guessFieldType.processStructGuess(format, consumerRecords);

                        int[] index = new int[1];
                        //                                    return colMeta.entrySet().stream().map((entry) -> {
                        //                                        return new ColumnMetaData(index[0]++, entry.getKey(), entry.getValue(), false);
                        //                                    }).collect(Collectors.toList());

                        return colMeta.entrySet().stream()//.filter((e) -> StructuredRecord.DEFAUTL_TABLE_NAME.equals(e.getKey()) || targetTabs.contains(e.getKey()))
                                .collect(Collectors.toMap((e) -> {
                                            index[0] = 0;
                                            return e.getKey();
                                        }
                                        , (e) -> e.getValue().entrySet().stream()
                                                .map((colEntry) -> new ColumnMetaData(index[0]++, colEntry.getKey(), colEntry.getValue(), false))
                                                .collect(Collectors.toList()), (u, v) -> {
                                            throw new IllegalStateException(String.format("Duplicate key %s", u));
                                        }, () -> new TreeMap<>(String.CASE_INSENSITIVE_ORDER)));

                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                }
            });

            List<ColumnMetaData> cols = tabColMeta.get(table.getTableName());
            if (CollectionUtils.isEmpty(cols)) {
                tableColsTypeCache.invalidate(this.dataXName);
                throw TisException.create("扫描Kafka历史消息，没有发现表：" + table.getTableName()
                        + "所对应的记录，当前扫描到的表记录为：“" + String.join(",", tabColMeta.keySet()) + "”，请对Topic中导入相应记录再执行此操作");
            }
            return cols;
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        }
    }

    private static final Method getKafkaSubscriberMethod;

    static {
        try {
            getKafkaSubscriberMethod = KafkaSource.class.getDeclaredMethod("getKafkaSubscriber");
            getKafkaSubscriberMethod.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    private KafkaSubscriber getTopicSubscriber() {
        try {
            KafkaSourceBuilder<DTO> kafkaSourceBuilder = this.createKafkaSourceBuilder(Collections.emptyMap());
            subscription.setSubscription(kafkaSourceBuilder);
            KafkaSource<DTO> kafkaSource = kafkaSourceBuilder.build();
            return (KafkaSubscriber) getKafkaSubscriberMethod.invoke(kafkaSource);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private class GuessStructuredReader extends StructuredReader<StructuredRecord> {
        private Iterator<ConsumerRecord<byte[], byte[]>> iterator;

        private final KafkaStructuredRecord reuseRecord;

        public GuessStructuredReader(ConsumerRecords<byte[], byte[]> consumerRecords) {
            this.iterator = Objects.requireNonNull(consumerRecords, "consumerRecords").iterator();
            this.reuseRecord = new KafkaStructuredRecord();
        }


        @Override
        public boolean hasNext() {
            return iterator.hasNext();
        }

        @Override
        public StructuredRecord next() {
            ConsumerRecord<byte[], byte[]> next = iterator.next();
            return DataXKafkaReader.this.format.parseRecord(this.reuseRecord, next.value());
        }
    }


    @Override
    public List<SelectedTab> getSelectedTabs() {

        if (this.selectedTabs == null) {
            this.selectedTabs = this.getTargetTabsEntities()
                    .stream().map((tab) -> {
                        return new SelectedTab(tab);
                    }).collect(Collectors.toList());
        }

        return this.selectedTabs;
    }

    @Override
    public void startScanDependency() {

    }

    public KafkaSourceBuilder<DTO> createKafkaSourceBuilder(
            Map<String, Map<String, Function<RunningContext, Object>>> contextParamValsGetterMapper) {
        KafkaSourceBuilder<DTO> kafkaSourceBuilder
                = KafkaSource.<DTO>builder()
                .setProperties(this.buildKafkaProperties());
        kafkaSourceBuilder.setValueOnlyDeserializer(
                new KafkaDTODeserializationSchema(this.format, contextParamValsGetterMapper));
        return kafkaSourceBuilder;
    }


    public Properties buildKafkaProperties() {
        Map<String, Object> props = buildKafkaConfig(false);
        Properties result = new Properties();
        props.forEach((key, val) -> {
            if (val != null) {
                result.setProperty(key, String.valueOf(val));
            }
        });
        return result;
    }

    public Map<String, Object> buildKafkaConfig(boolean isTest) {
        Builder<String, Object> builder = ImmutableMap.<String, Object>builder();
        builder
                .put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, this.bootstrapServers)
                .putAll(protocol.propertiesByProtocol())
                .put(ConsumerConfig.CLIENT_ID_CONFIG,
                        StringUtils.isNotBlank(clientId) ? clientId : StringUtils.EMPTY)
                .put(ConsumerConfig.RETRY_BACKOFF_MS_CONFIG, this.retryBackoffMs)
                .put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, this.autoCommitIntervalMs)
                //.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, this.autoOffsetReset)
                .put(ConsumerConfig.CLIENT_DNS_LOOKUP_CONFIG, this.clientDnsLookup)
                // .put(ConsumerConfig.BUFFER_MEMORY_CONFIG, bufferMemory)
                // .put(ConsumerConfig.GROUP_ID_CONFIG, this.groupId)
                .put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, this.maxPollRecords)
                .put(ConsumerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MS_CONFIG, socketConnectionSetupTimeoutMs)
                .put(ConsumerConfig.SOCKET_CONNECTION_SETUP_TIMEOUT_MAX_MS_CONFIG, socketConnectionSetupTimeoutMaxMs)
                .put(ConsumerConfig.REQUEST_TIMEOUT_MS_CONFIG, requestTimeoutMs)
                .put(ConsumerConfig.RECEIVE_BUFFER_CONFIG, receiveBufferBytes)
                .put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName())//   .StringSerializer.class.getName())
                .put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());// JsonSerializer.class.getName())

        if (StringUtils.isNotBlank(this.groupId)) {
            builder.put(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
            builder.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, (this.enableAutoCommit));
        } else {
            // groupId 为空的情况下，autoCommitConfig 必须为false
            builder.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);
        }

        final Map<String, Object> props = builder.build();
        return props.entrySet().stream()
                .filter(entry -> entry.getValue() != null && StringUtils.isNotBlank(entry.getValue().toString()))
                .collect(Collectors.toMap((e) -> e.getKey(), (e) -> String.valueOf(e.getValue())));
    }


    @Override
    public String getTemplate() {
        return null;
    }

    @TISExtension
    public static class DefaultDescriptor extends BaseDataxReaderDescriptor {
        public DefaultDescriptor() {
            super();
        }

        @Override
        protected final boolean validateAll(
                IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            // DataXKafkaReader kafkaReader = postFormVals.newInstance();
            DataXKafkaReader kafkaReader = createDataXKafkaReader(msgHandler, postFormVals);
            if (!kafkaReader.format.validateFormtField(msgHandler, context, FormatFactory.KEY_FIELD_FORMAT, kafkaReader)) {
                return false;
            }

            try {
                for (String targetTab : kafkaReader.format.parseTargetTabsEntities()) {
                    kafkaReader.parseTableMetadataFromKafkaHistoryEvents(EntityName.parse(targetTab));
                }
            } catch (Exception e) {
                logger.warn("pipelineName:" + kafkaReader.dataXName, e);
                ErrMsg errMsg = TisException.getErrMsg(e);
                msgHandler.addFieldError(context, FIELD_KEY_FORMAT, errMsg.getMessage());
                return false;
            }


            return super.validateAll(msgHandler, context, postFormVals);
        }

        @Override
        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {

            DataXKafkaReader dataxReader = createDataXKafkaReader(msgHandler, postFormVals);
            if (StringUtils.isEmpty(dataxReader.testTopic)) {
                msgHandler.addFieldError(context, "testTopic", ValidatorCommons.MSG_EMPTY_INPUT_ERROR);
                return false;
            }

            try {
                final String testTopic = dataxReader.testTopic; //config.has("test_topic") ? config.get("test_topic").asText() : "";

                final KafkaConsumerFactory kafkaFactory = dataxReader.createKafkaFactory();// KafkaConsumerFactory.getKafkaConfig(dataxReader, true);
                try (KafkaConsumer<byte[], byte[]> consumer = kafkaFactory.getConsumer()) {
                    consumer.subscribe(Pattern.compile(testTopic));
                    consumer.listTopics();
                }
//                catch (final Exception e) {
//                    msgHandler.addFieldError(context, "testTopic", e.getMessage());
//                    return false;
//                }


            } catch (final Exception e) {

                if (ExceptionUtils.indexOfThrowable(e, TimeoutException.class) > -1) {
                    throw TisException.create("Kafka服务端连接超时，请检查相关配置是否正确,详细：" + e.getMessage(), e);
                }

                logger.error("Exception attempting to connect to the Kafka brokers: ", e);
                msgHandler.addErrorMessage(context, "Could not connect to the Kafka brokers with provided configuration. \n" + e.getMessage());

                return false;
            }


            return true;
        }

        private DataXKafkaReader createDataXKafkaReader(IControlMsgHandler msgHandler, PostFormVals postFormVals) {
            DataXKafkaReader dataxReader
                    = postFormVals.newInstance();
            if (!msgHandler.isCollectionAware()) {
                throw new IllegalStateException("current context must collectionAware");
            }
            dataxReader.dataXName = msgHandler.getCollectionName().getPipelineName();
            return dataxReader;
        }


        @Override
        public boolean isRdbms() {
            return true;
        }

        @Override
        public boolean isSupportIncr() {
            return true;
        }

        @Override
        public boolean isSupportBatch() {
            return false;
        }

        @Override
        public EndType getEndType() {
            return EndType.Kafka;
        }

        @Override
        public String getDisplayName() {
            return getEndType().name();
        }
    }


}
