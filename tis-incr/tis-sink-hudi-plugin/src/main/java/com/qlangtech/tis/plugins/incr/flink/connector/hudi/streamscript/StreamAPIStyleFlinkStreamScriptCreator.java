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
import com.qlangtech.tis.plugin.datax.hudi.DataXHudiWriter;
import com.qlangtech.tis.plugin.datax.hudi.HudiSelectedTab;
import com.qlangtech.tis.plugin.datax.hudi.HudiTableMeta;
import com.qlangtech.tis.plugins.incr.flink.connector.hudi.HudiSinkFactory;
import com.qlangtech.tis.sql.parser.visitor.BlockScriptBuffer;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-03-24 11:02
 **/
public class StreamAPIStyleFlinkStreamScriptCreator extends BasicFlinkStreamScriptCreator {
    public StreamAPIStyleFlinkStreamScriptCreator(HudiSinkFactory hudiSinkFactory) {
        super(hudiSinkFactory);
    }

    @Override
    public String getFlinkStreamGenerateTemplateFileName() {
        return TEMPLATE_FLINK_HUDI_STREAM_STYLE_HANDLE_SCALA;
    }

    @Override
    public IStreamTemplateData decorateMergeData(IStreamTemplateData mergeData) {
        return null;
    }

    public class HudiStreamTemplateData extends AdapterStreamTemplateData {
        public HudiStreamTemplateData(IStreamTemplateData data) {
            super(data);
        }

        public String getFlinkStreamerConfig(String tableName) {
            BlockScriptBuffer script = new BlockScriptBuffer();
            // Pair<HudiSelectedTab, HudiTableMeta> tableMeta = hudiSinkFactory.getTableMeta(tableName);
            createStreamerConfig(tableName, script, hudiSinkFactory);
            return script.toString();
        }

    }


    private void createStreamerConfig(String tabName, BlockScriptBuffer script, HudiSinkFactory sinkFuncFactory) {
        // final FlinkStreamerConfig cfg = new FlinkStreamerConfig();
        DataXHudiWriter hudiWriter = HudiSinkFactory.getDataXHudiWriter(sinkFuncFactory);
        IHiveConnGetter hiveMeta = hudiWriter.getHiveConnMeta();
        Pair<HudiSelectedTab, HudiTableMeta> tableMeta = sinkFuncFactory.getTableMeta(tabName);
        HudiSelectedTab hudiTab = tableMeta.getLeft();
        ITISFileSystem fs = hudiWriter.getFs().getFileSystem();
        IPath dumpDir = HudiTableMeta.getDumpDir(fs, tabName, sinkFuncFactory.dumpTimeStamp, hiveMeta);
        // ITISFileSystem fs, IHiveConnGetter hiveConn, String tabName, String dumpTimeStamp
        script.appendLine("// table "+tabName+" relevant Flink config");
        script.appendLine("cfg.sourceAvroSchemaPath = %s", String.valueOf(HudiTableMeta.getTableSourceSchema(fs, dumpDir)));
        script.appendLine("cfg.targetBasePath = %s", String.valueOf(HudiTableMeta.getHudiDataDir(fs, dumpDir)));
        script.appendLine("cfg.targetTableName = %s", tabName);
        script.appendLine("cfg.tableType = %s", hudiWriter.getHudiTableType().getValue());
        script.appendLine("cfg.operation = BatchOpMode.parse(%s).hudiType ", BatchOpMode.parse(hudiWriter.batchOp).hudiType);
        script.appendLine("cfg.preCombine = true");
        script.appendLine("cfg.sourceOrderingField = %s", hudiTab.sourceOrderingField);
        script.appendLine("cfg.recordKeyField = %s", hudiTab.recordField);

        // cfg.partitionPathField =
        this.setPartitionRelevantProps(script, hudiTab, hudiWriter);
        script.appendLine("cfg.writeRateLimit = %sl", sinkFuncFactory.currentLimit);

        script.appendLine("cfg.hiveSyncEnabled = true");
        script.appendLine("cfg.hiveSyncDb = %s", hiveMeta.getDbName());
        script.appendLine("cfg.hiveSyncTable = %s", tabName);
        script.appendLine("cfg.hiveSyncMode = %s", HudiSinkFactory.HIVE_SYNC_MODE);
        script.appendLine("cfg.hiveSyncMetastoreUri = %s", hiveMeta.getMetaStoreUrls());
    }

    private void setPartitionRelevantProps(BlockScriptBuffer script, HudiSelectedTab hudiTab, DataXHudiWriter hudiWriter) {
        hudiTab.partition.setProps((key, val) -> {
            if (StringUtils.isEmpty(val)) {
                return;
            }
            switch (key) {
                case IPropertiesBuilder.KEY_HOODIE_DATASOURCE_HIVE_SYNC_PARTITION_EXTRACTOR_CLASS:
                    script.appendLine("cfg.hiveSyncPartitionExtractorClass = %s", val);
                    break;
                case IPropertiesBuilder.KEY_HOODIE_DATASOURCE_HIVE_SYNC_PARTITION_FIELDS:
                    script.appendLine("cfg.partitionPathField = %s", val);
                    break;
                case IPropertiesBuilder.KEY_HOODIE_DATASOURCE_WRITE_KEYGENERATOR_TYPE:
                    script.appendLine("cfg.keygenType = %s", val);
                    break;
                case IPropertiesBuilder.KEY_HOODIE_PARTITIONPATH_FIELD:
                    script.appendLine("cfg.partitionDefaultName = %s", val);
                    break;
                default:
                    throw new IllegalStateException("key:" + key + " is illegal");
            }
        }, hudiWriter);
    }
}
