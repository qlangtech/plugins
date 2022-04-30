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

package com.qlangtech.tis.plugins.incr.flink.connector.hudi.streamscript;

import com.alibaba.datax.plugin.writer.hudi.IPropertiesBuilder;
import com.qlangtech.tis.config.hive.IHiveConnGetter;
import com.qlangtech.tis.fs.IPath;
import com.qlangtech.tis.fs.ITISFileSystem;
import com.qlangtech.tis.plugin.datax.hudi.BatchOpMode;
import com.qlangtech.tis.plugin.datax.hudi.HudiSelectedTab;
import com.qlangtech.tis.plugin.datax.hudi.HudiTableMeta;
import com.qlangtech.tis.plugin.datax.hudi.IDataXHudiWriter;
import com.qlangtech.tis.plugins.incr.flink.connector.hudi.HudiSinkFactory;
import com.qlangtech.tis.plugins.incr.flink.connector.hudi.compaction.CompactionConfig;
import com.qlangtech.tis.sql.parser.visitor.BlockScriptBuffer;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-03-24 11:02
 **/
public class StreamAPIStyleFlinkStreamScriptCreator extends BasicFlinkStreamScriptCreator {

    private static final Logger logger = LoggerFactory.getLogger(StreamAPIStyleFlinkStreamScriptCreator.class);

    public StreamAPIStyleFlinkStreamScriptCreator(HudiSinkFactory hudiSinkFactory) {
        super(hudiSinkFactory);
    }

    @Override
    public String getFlinkStreamGenerateTemplateFileName() {
        return TEMPLATE_FLINK_HUDI_STREAM_STYLE_HANDLE_SCALA;
    }

    @Override
    public IStreamTemplateData decorateMergeData(IStreamTemplateData mergeData) {
        return new HudiStreamTemplateData(mergeData);
    }

    public class HudiStreamTemplateData extends AdapterStreamTemplateData {
        private final IDataXHudiWriter hudiWriter;

        public HudiStreamTemplateData(IStreamTemplateData data) {
            super(data);
            this.hudiWriter = HudiSinkFactory.getDataXHudiWriter(hudiSinkFactory);
        }

        public String getFlinkStreamerConfig(String tableName) {
            BlockScriptBuffer script = new BlockScriptBuffer(BlockScriptBuffer.INDENT_STEP);
            // Pair<HudiSelectedTab, HudiTableMeta> tableMeta = hudiSinkFactory.getTableMeta(tableName);
            createStreamerConfig(tableName, script, hudiSinkFactory);
            return script.toString();
        }

        public String getHudiOperationType() {
            return BatchOpMode.parse(hudiSinkFactory.opType).hudiType;
        }

        private void createStreamerConfig(String tabName, BlockScriptBuffer script, HudiSinkFactory sinkFuncFactory) {
            // final FlinkStreamerConfig cfg = new FlinkStreamerConfig();

            IHiveConnGetter hiveMeta = hudiWriter.getHiveConnMeta();
            Pair<HudiSelectedTab, HudiTableMeta> tableMeta = sinkFuncFactory.getTableMeta(tabName);
            HudiSelectedTab hudiTab = tableMeta.getLeft();
            ITISFileSystem fs = hudiWriter.getFileSystem();
            IPath dumpDir = HudiTableMeta.getDumpDir(fs, tabName, sinkFuncFactory.dumpTimeStamp, hiveMeta);
            // ITISFileSystem fs, IHiveConnGetter hiveConn, String tabName, String dumpTimeStamp

            script.appendLine("// table " + tabName + " relevant Flink config");
            script.appendLine("cfg.sourceAvroSchemaPath = %s", String.valueOf(HudiTableMeta.getTableSourceSchema(fs, dumpDir)));
            script.appendLine("cfg.targetBasePath = %s", String.valueOf(HudiTableMeta.getHudiDataDir(fs, dumpDir)));
            script.appendLine("cfg.targetTableName = %s", tabName);
            script.appendLine("cfg.tableType = %s", hudiWriter.getHudiTableType().getValue());
            // script.appendLine("cfg.operation = %s ", BatchOpMode.parse(hudiWriter.batchOp).hudiType);
            script.appendLine("cfg.preCombine = true");
            script.appendLine("cfg.sourceOrderingField = %s", hudiTab.sourceOrderingField);
            script.appendLine("cfg.recordKeyField = %s", hudiTab.recordField);

            // cfg.partitionPathField =
            setPartitionRelevantProps(script, hudiTab, hudiWriter);
            script.appendLine("cfg.writeRateLimit = %sl", sinkFuncFactory.currentLimit);

            script.appendLine("cfg.hiveSyncEnabled = true");
            script.appendLine("cfg.hiveSyncDb = %s", hiveMeta.getDbName());
            script.appendLine("cfg.hiveSyncTable = %s", tabName);
            script.appendLine("cfg.hiveSyncMode = %s", HudiSinkFactory.HIVE_SYNC_MODE);
            script.appendLine("cfg.hiveSyncMetastoreUri = %s", hiveMeta.getMetaStoreUrls());


            CompactionConfig compaction = sinkFuncFactory.compaction;
            if (compaction != null) {


//                public String payloadClass;
//
//
//                public Integer targetIOPerInMB;
//
//
//                public String triggerStrategy;
//
//
//                public Integer maxNumDeltaCommitsBefore;
//
//
//                public Integer maxDeltaSecondsBefore;
//
//
//                public Boolean asyncClean;
//
//
//                public Integer retainCommits;
//
//
//                public Integer archiveMinCommits;
//
//
//                public Integer archiveMaxCommits;
                script.appendLine("cfg.payloadClassName = %s", compaction.payloadClass);
                script.appendLine("cfg.compactionTargetIo = %s", compaction.targetIOPerInMB);
                script.appendLine("cfg.compactionTriggerStrategy = %s", compaction.triggerStrategy);
                script.appendLine("cfg.compactionDeltaCommits = %s", compaction.maxNumDeltaCommitsBefore);
                script.appendLine("cfg.compactionDeltaSeconds = %s", compaction.maxDeltaSecondsBefore);
                script.appendLine("cfg.cleanAsyncEnabled = %s", compaction.asyncClean);
                script.appendLine("cfg.cleanRetainCommits = %s", compaction.retainCommits);
                script.appendLine("cfg.archiveMinCommits = %s", compaction.archiveMinCommits);
                script.appendLine("cfg.archiveMaxCommits = %s", compaction.archiveMaxCommits);
            }


            // CompactionConfig compact = sinkFuncFactory.compaction;
            //   compact.triggerStrategy;
        }
    }


    private void setPartitionRelevantProps(
            BlockScriptBuffer script, HudiSelectedTab hudiTab, IDataXHudiWriter hudiWriter) {
        if (hudiTab.partition == null) {
            throw new IllegalArgumentException("hudiTab.partition can not be null ");
        }
        hudiTab.partition.setProps((key, val) -> {
            if (StringUtils.isEmpty(val)) {
                return;
            }
            switch (key) {
                case IPropertiesBuilder.KEY_HOODIE_DATASOURCE_HIVE_SYNC_PARTITION_EXTRACTOR_CLASS:
                    script.appendLine("cfg.hiveSyncPartitionExtractorClass = %s", val);
                    break;
                case IPropertiesBuilder.KEY_HOODIE_DATASOURCE_HIVE_SYNC_PARTITION_FIELDS:
                   // script.appendLine("cfg.partitionDefaultName = %s", val);
                    script.appendLine("cfg.hiveSyncPartitionFields = %s", val);
                    break;
                case IPropertiesBuilder.KEY_HOODIE_DATASOURCE_WRITE_KEYGENERATOR_TYPE:
                    script.appendLine("cfg.keygenType = %s", val);
                    break;
                case IPropertiesBuilder.KEY_HOODIE_PARTITIONPATH_FIELD:
                    script.appendLine("cfg.partitionPathField = %s", val);
                    break;
//                props.setProperty(TimestampBasedAvroKeyGenerator.Config.TIMESTAMP_TYPE_FIELD_PROP, this.timestampType);
//                props.setProperty(TimestampBasedAvroKeyGenerator.Config.TIMESTAMP_INPUT_DATE_FORMAT_PROP, this.inputDateformat);
//                props.setProperty(TimestampBasedAvroKeyGenerator.Config.TIMESTAMP_OUTPUT_DATE_FORMAT_PROP, this.outputDateformat);
//                props.setProperty(TimestampBasedAvroKeyGenerator.Config.TIMESTAMP_TIMEZONE_FORMAT_PROP, this.timezone);
                case "hoodie.deltastreamer.keygen.timebased.timestamp.type":
                case "hoodie.deltastreamer.keygen.timebased.input.dateformat":
                case "hoodie.deltastreamer.keygen.timebased.output.dateformat":
                case "hoodie.deltastreamer.keygen.timebased.timezone":
                    // logger.warn("unSupport deltaStream param:{} value:{}", key, val);
                    script.appendLine("cfg.setString(%s , %s)", key, val);
                    break;
                default:
                    throw new IllegalStateException("key:" + key + " is illegal");
            }
        }, hudiWriter);
    }
}
