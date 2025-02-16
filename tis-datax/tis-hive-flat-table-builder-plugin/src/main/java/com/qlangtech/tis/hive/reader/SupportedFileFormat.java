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

package com.qlangtech.tis.hive.reader;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.qlangtech.tis.config.hive.meta.HiveTable.HiveTabColType;
import com.qlangtech.tis.config.hive.meta.IHiveTableParams;
import com.qlangtech.tis.hive.DefaultHiveMetaStore.HiveStoredAs;
import com.qlangtech.tis.hive.reader.impl.HadoopHFileInputFormat;
import com.qlangtech.tis.hive.reader.impl.HadoopOrcInputFormat;
import com.qlangtech.tis.hive.reader.impl.HadoopParquetInputFormat;
import com.qlangtech.tis.hive.reader.impl.HadoopTextInputFormat;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.datax.format.FileFormat;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.hbase.HiveHBaseTableInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcFileStripeMergeInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcNewInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputFormat;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-12-19 10:46
 **/
public abstract class SupportedFileFormat implements IdentityName {

    public static final String KEY_SUPPORTED_FORMAT_TEXT = "Text";
    public static final String KEY_SUPPORTED_FORMAT_HFILE = "HFile";
    public static final String KEY_SUPPORTED_FORMAT_PARQUET = "Parquet";
    public static final String KEY_SUPPORTED_FORAMT_ORC = "Orc";

    public abstract boolean match(Class<?> inputFormatClass);


    /**
     * 创建对应文件格式的读/写器
     *
     * @param entityName
     * @param cols
     * @param storedAs
     * @return
     * @throws Exception
     */
    public abstract FileFormat createFileFormatReader(
            String entityName, List<HiveTabColType> cols, HiveStoredAs storedAs, IHiveTableParams tableParams) throws Exception;

    private static final List<SupportedFileFormat> supportedFileFormat;

    static {
        Builder<SupportedFileFormat> supportedFileFormatBuilder = ImmutableList.builder();
        supportedFileFormatBuilder.add(new TextFileFormat());
        supportedFileFormatBuilder.add(new HFileFileFormat());
        supportedFileFormatBuilder.add(new ParquetFileFormat());
        supportedFileFormatBuilder.add(new OrcFileFormat());
        supportedFileFormat = supportedFileFormatBuilder.build();
    }

    public static List<SupportedFileFormat> supportedFileFormat() {
        // SupportedFileFormat.
        return supportedFileFormat;
    }

    /**
     * 在write中使用
     *
     * @param format
     * @return
     */
    public static SupportedFileFormat parse(IdentityName format) {
        List<SupportedFileFormat> formats = getSupportedFileFormat(Collections.singletonList(format.identityValue()));
        for (SupportedFileFormat supportFormat : formats) {
            return supportFormat;
        }
        throw new IllegalStateException("can not find format:" + format.identityValue() + " in "
                + supportedFileFormat.stream().map((f) -> f.identityValue()).collect(Collectors.joining(",")));
    }

    public static List<SupportedFileFormat> getSupportedFileFormat(List<String> fileFormat) {
        if (CollectionUtils.isEmpty(fileFormat)) {
            throw new IllegalArgumentException("param fileFormat can not be empty");
        }
        List<SupportedFileFormat> result = Lists.newArrayList();
        Set<String> targetFileFormat = Sets.newHashSet(fileFormat);
        for (SupportedFileFormat format : supportedFileFormat) {

            if (targetFileFormat.contains(format.identityValue())) {
                result.add(format);
            }
//            if (StringUtils.equals(format.identityValue(), fileFormat)) {
//                return format;
//            }
        }
        if (CollectionUtils.isEmpty(result)) {
            throw new IllegalArgumentException("can not find matched fileFormat:" + String.join(",", targetFileFormat) + " capacity format:"
                    + supportedFileFormat.stream().map((f) -> f.identityValue()).collect(Collectors.joining(",")));
        }

        return result;

    }

    /**
     * <pre>
     *        if (org.apache.hadoop.mapred.TextInputFormat.class.isAssignableFrom(inputFormatClass)) {
     *                 org.apache.hadoop.mapred.TextInputFormat inputFormat
     *                         = (org.apache.hadoop.mapred.TextInputFormat) inputFormatClass.getDeclaredConstructor().newInstance();
     *                 inputFormat.configure(jobConf);
     *                 return new HadoopTextInputFormat(entityName, cols.size(), inputFormat, serde, jobConf);
     *             } else if (MapredParquetInputFormat.class == inputFormatClass) {
     *                 MapredParquetInputFormat pqInputFormat = (MapredParquetInputFormat) inputFormatClass.getDeclaredConstructor().newInstance();
     *                 return new HadoopParquetInputFormat(entityName, cols.size(), pqInputFormat, serde, jobConf);
     *             } else if(HFileInputFormat.class == inputFormatClass){
     *
     *                 HiveHBaseTableInputFormat hfileInputFormat
     *                         = (HiveHBaseTableInputFormat) inputFormatClass.getDeclaredConstructor().newInstance();
     *                 return new HadoopHFileInputFormat(entityName, cols.size(), hfileInputFormat, serde, jobConf);
     *             }
     *             else {
     *                 throw new IllegalStateException("outputFormatClass:" + inputFormatClass.getName() + " can not be resolved");
     *             }
     * </pre>
     */


    private static class TextFileFormat extends SupportedFileFormat {
        @Override
        public boolean match(Class<?> inputFormatClass) {
            return org.apache.hadoop.mapred.TextInputFormat.class.isAssignableFrom(inputFormatClass);
        }

        // HiveStoredAs storedAs
        @Override
        public FileFormat createFileFormatReader(String entityName, List<HiveTabColType> cols
                , HiveStoredAs serde, IHiveTableParams tableParams) throws Exception {
//            org.apache.hadoop.mapred.TextInputFormat inputFormat
//                    = (org.apache.hadoop.mapred.TextInputFormat) inputFormatClass.getDeclaredConstructor().newInstance();
//            inputFormat.configure(jobConf);
//
//            FileOutputFormat outputFormat = (FileOutputFormat) outputFormatClass.getDeclaredConstructor().newInstance();
            return new HadoopTextInputFormat(entityName, cols.size(), serde, tableParams);
        }

        @Override
        public String identityValue() {
            return KEY_SUPPORTED_FORMAT_TEXT;
        }
    }

    /**
     * support hbase file format
     */
    private static class HFileFileFormat extends SupportedFileFormat {
        @Override
        public boolean match(Class<?> inputFormatClass) {
            return HiveHBaseTableInputFormat.class == inputFormatClass;

        }

        @Override
        public FileFormat createFileFormatReader(
                String entityName, List<HiveTabColType> cols, HiveStoredAs serde, IHiveTableParams tableParams) throws Exception {
//            HiveHBaseTableInputFormat hfileInputFormat
//                    = (HiveHBaseTableInputFormat) inputFormatClass.getDeclaredConstructor().newInstance();
//            FileOutputFormat outputFormat = (FileOutputFormat) outputFormatClass.getDeclaredConstructor().newInstance();
            return new HadoopHFileInputFormat(entityName, cols.size(), serde, tableParams);
        }

        @Override
        public String identityValue() {
            return KEY_SUPPORTED_FORMAT_HFILE;
        }
    }

    private static class ParquetFileFormat extends SupportedFileFormat {
        @Override
        public boolean match(Class<?> inputFormatClass) {
            return MapredParquetInputFormat.class == inputFormatClass;
        }

        @Override
        public FileFormat createFileFormatReader(
                String entityName, List<HiveTabColType> cols, HiveStoredAs serde, IHiveTableParams tableParams) throws Exception {
//            MapredParquetInputFormat pqInputFormat = (MapredParquetInputFormat) inputFormatClass.getDeclaredConstructor().newInstance();
//            FileOutputFormat outputFormat = (FileOutputFormat) outputFormatClass.getDeclaredConstructor().newInstance();
            return new HadoopParquetInputFormat(entityName, cols.size(), serde, tableParams);
        }

        @Override
        public String identityValue() {
            return KEY_SUPPORTED_FORMAT_PARQUET;
        }
    }


    private static class OrcFileFormat extends SupportedFileFormat {
        @Override
        public boolean match(Class<?> inputFormatClass) {
            return OrcFileStripeMergeInputFormat.class == inputFormatClass
                    || OrcInputFormat.class == inputFormatClass
                    || OrcNewInputFormat.class == inputFormatClass;
        }

        @Override
        public FileFormat createFileFormatReader(
                String entityName, List<HiveTabColType> cols
                , HiveStoredAs serde, IHiveTableParams tableParams) throws Exception {
//            OrcFileStripeMergeInputFormat ocrInputFormat = (OrcFileStripeMergeInputFormat) inputFormatClass.getDeclaredConstructor().newInstance();
//            FileOutputFormat outputFormat = (FileOutputFormat) outputFormatClass.getDeclaredConstructor().newInstance();
            return new HadoopOrcInputFormat(entityName, cols.size(), serde, tableParams);
        }

        @Override
        public String identityValue() {
            return KEY_SUPPORTED_FORAMT_ORC;
        }
    }


}
