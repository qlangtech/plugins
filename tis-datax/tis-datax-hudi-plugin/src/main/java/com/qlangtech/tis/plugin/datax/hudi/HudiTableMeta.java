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
import com.qlangtech.tis.config.hive.IHiveConnGetter;
import com.qlangtech.tis.fs.IPath;
import com.qlangtech.tis.fs.ITISFileSystem;
import com.qlangtech.tis.offline.DataxUtils;
import org.apache.commons.collections.CollectionUtils;

import java.util.List;

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

    public IPath getDumpDir(ITISFileSystem fs, IHiveConnGetter hiveConn) {
        if (this.tabDumpDir == null) {
            this.tabDumpDir = fs.getPath(fs.getRootDir(), hiveConn.getDbName() + "/" + this.hudiTabName);
        }
        return this.tabDumpDir;
    }

    public IPath getHudiDataDir(ITISFileSystem fs, IHiveConnGetter hiveConn) {
        return fs.getPath(getDumpDir(fs, hiveConn), "hudi");
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
