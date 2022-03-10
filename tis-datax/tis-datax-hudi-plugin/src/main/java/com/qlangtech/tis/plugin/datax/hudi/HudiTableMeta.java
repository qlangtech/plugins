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

package com.qlangtech.tis.plugin.datax.hudi;

import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.plugin.writer.hdfswriter.HdfsColMeta;
import com.alibaba.datax.plugin.writer.hdfswriter.HdfsWriterErrorCode;
import com.alibaba.datax.plugin.writer.hdfswriter.Key;
import com.alibaba.datax.plugin.writer.hdfswriter.SupportHiveDataType;
import com.qlangtech.tis.config.hive.IHiveConnGetter;
import com.qlangtech.tis.fs.IPath;
import com.qlangtech.tis.fs.IPathInfo;
import com.qlangtech.tis.fs.ITISFileSystem;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.offline.DataxUtils;
import com.qlangtech.tis.plugin.datax.BasicHdfsWriterJob;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.IOUtils;

import java.io.OutputStream;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-02-20 11:21
 **/
public class HudiTableMeta {
    public static final String KEY_SOURCE_ORDERING_FIELD = "hudiSourceOrderingField";
    public final List<HdfsColMeta> colMetas;
    private final String sourceOrderingField;
    private final String dataXName;
    private final String pkName;
    private final String partitionpathField;
    private final int shuffleParallelism;
    private final HudiWriteTabType hudiTabType;
    private final String hudiTabName;
    private IPath tabDumpDir = null;

    public static IPath createFsSourceSchema(ITISFileSystem fs
            , String tabName, IPath tabDumpDir, HudiSelectedTab hudiTabMeta) {

        List<ISelectedTab.ColMeta> colsMetas = hudiTabMeta.getCols();
        IPath fsSourceSchemaPath = fs.getPath(tabDumpDir, "meta/schema.avsc");

        try (OutputStream schemaWriter = fs.getOutputStream(fsSourceSchemaPath)) {
            SchemaBuilder.RecordBuilder<Schema> builder = SchemaBuilder.record(tabName);
            SchemaBuilder.FieldAssembler<Schema> fields = builder.fields();

            for (ISelectedTab.ColMeta meta : colsMetas) {
                SupportHiveDataType hiveDataType = DataType.convert2HiveType(meta.getType());
                switch (hiveDataType) {
                    case STRING:
                    case DATE:
                    case TIMESTAMP:
                    case VARCHAR:
                    case CHAR:
                        // fields.nullableString(meta.colName, StringUtils.EMPTY);
//                            if (meta.nullable) {
//                                fields.nullableString(meta.colName, StringUtils.EMPTY);
//                            } else {
                        // fields.requiredString(meta.colName);
                        // SchemaBuilder.StringDefault<Schema> strType = fields.name(meta.colName).type().stringType();
                        if (meta.isNullable()) {
                            // strType.stringDefault(StringUtils.EMPTY);
                            fields.optionalString(meta.getName());
                        } else {
                            //   strType.noDefault();
                            fields.requiredString(meta.getName());
                        }
                        //}
                        break;
                    case DOUBLE:
                        if (meta.isNullable()) {
                            fields.optionalDouble(meta.getName());
                        } else {
                            fields.requiredDouble(meta.getName());
                        }
                        break;
                    case INT:
                    case TINYINT:
                    case SMALLINT:
                        if (meta.isNullable()) {
                            fields.optionalInt(meta.getName());
                        } else {
                            fields.requiredInt(meta.getName());
                        }
                        break;
                    case BOOLEAN:
                        if (meta.isNullable()) {
                            fields.optionalBoolean(meta.getName());
                        } else {
                            fields.requiredBoolean(meta.getName());
                        }
                        break;
                    case BIGINT:
                        if (meta.isNullable()) {
                            fields.optionalLong(meta.getName());
                        } else {
                            fields.requiredLong(meta.getName());
                        }
                        break;
                    case FLOAT:
                        if (meta.isNullable()) {
                            fields.optionalFloat(meta.getName());
                        } else {
                            fields.requiredFloat(meta.getName());
                        }
                        break;
                    default:
                        throw new IllegalStateException("illegal type:" + hiveDataType);
                }
            }

            Schema schema = fields.endRecord();

            if (schema.getFields().size() != colsMetas.size()) {
                throw new IllegalStateException("schema.getFields():" + schema.getFields().size() + " is not equal to 'colsMeta.size()':" + colsMetas.size());
            }
            IOUtils.write(schema.toString(true), schemaWriter, TisUTF8.get());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        return fsSourceSchemaPath;
    }


    public boolean isColsEmpty() {
        return CollectionUtils.isEmpty(this.colMetas);
    }

    public HudiTableMeta(Configuration paramCfg) {
        this.colMetas = HdfsColMeta.getColsMeta(paramCfg);
        if (this.isColsEmpty()) {
            throw new IllegalStateException("colMetas can not be null");
        }

        this.hudiTabName = paramCfg.getNecessaryValue(Key.FILE_NAME, HdfsWriterErrorCode.REQUIRED_VALUE);
        this.sourceOrderingField
                = paramCfg.getNecessaryValue(KEY_SOURCE_ORDERING_FIELD, HdfsWriterErrorCode.REQUIRED_VALUE);
        this.dataXName = paramCfg.getNecessaryValue(DataxUtils.DATAX_NAME, HdfsWriterErrorCode.REQUIRED_VALUE);
        this.pkName = paramCfg.getNecessaryValue("hudiRecordkey", HdfsWriterErrorCode.REQUIRED_VALUE);
        this.partitionpathField = paramCfg.getNecessaryValue("hudiPartitionpathField", HdfsWriterErrorCode.REQUIRED_VALUE);
        this.shuffleParallelism
                = Integer.parseInt(paramCfg.getNecessaryValue("shuffleParallelism", HdfsWriterErrorCode.REQUIRED_VALUE));
        this.hudiTabType = HudiWriteTabType.parse(paramCfg.getNecessaryValue("hudiTabType", HdfsWriterErrorCode.REQUIRED_VALUE));
    }

    public IPath getDumpDir(BasicHdfsWriterJob writerJob, IHiveConnGetter hiveConn) {
        return getDumpDir(writerJob.getFileSystem(), writerJob.getDumpTimeStamp(), hiveConn);
    }

    public IPath getDumpDir(ITISFileSystem fs, String dumpTimeStamp, IHiveConnGetter hiveConn) {
        if (this.tabDumpDir == null) {
            this.tabDumpDir = getDumpDir(fs, hudiTabName, dumpTimeStamp, hiveConn);// fs.getPath(fs.getRootDir(), hiveConn.getDbName() + "/" + dumpTimeStamp + "/" + this.hudiTabName);
        }
        return this.tabDumpDir;
    }

    public static IPath getDumpDir(ITISFileSystem fs, String hudiTabName, String dumpTimeStamp, IHiveConnGetter hiveConn) {
        return fs.getPath(fs.getRootDir(), hiveConn.getDbName() + "/" + dumpTimeStamp + "/" + hudiTabName);
    }

    public static List<Option> getHistoryBatchs(ITISFileSystem fs, IHiveConnGetter hiveConn) {
        IPath path = fs.getPath(fs.getRootDir(), hiveConn.getDbName());
        List<IPathInfo> child = fs.listChildren(path);
        return child.stream().map((c) -> new Option(c.getName())).collect(Collectors.toList());
    }

    public IPath getHudiDataDir(ITISFileSystem fs, String dumpTimeStamp, IHiveConnGetter hiveConn) {
        return fs.getPath(getDumpDir(fs, dumpTimeStamp, hiveConn), "hudi");
    }

    public String getSourceOrderingField() {
        return sourceOrderingField;
    }

    public String getDataXName() {
        return dataXName;
    }

    public String getPkName() {
        return pkName;
    }

    public String getPartitionpathField() {
        return partitionpathField;
    }

    public int getShuffleParallelism() {
        return shuffleParallelism;
    }

    public HudiWriteTabType getHudiTabType() {
        return hudiTabType;
    }
}
