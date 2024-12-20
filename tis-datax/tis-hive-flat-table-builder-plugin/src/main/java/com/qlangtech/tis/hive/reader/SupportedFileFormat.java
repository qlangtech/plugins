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
import com.qlangtech.tis.config.hive.meta.HiveTable.HiveTabColType;
import com.qlangtech.tis.hive.reader.impl.HadoopHFileInputFormat;
import com.qlangtech.tis.hive.reader.impl.HadoopParquetInputFormat;
import com.qlangtech.tis.hive.reader.impl.HadoopTextInputFormat;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.datax.format.FileFormat;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.hbase.HiveHBaseTableInputFormat;
import org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.mapred.JobConf;

import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-12-19 10:46
 **/
public abstract class SupportedFileFormat implements IdentityName {

    public abstract boolean match(Class<?> inputFormatClass);

    //public abstract com.qlangtech.tis.plugin.datax.format.FileFormat createFileFormatReader();
    public abstract FileFormat createFileFormatReader(
            String entityName, List<HiveTabColType> cols, AbstractSerDe serde, Class<?> inputFormatClass, JobConf jobConf) throws Exception;

    private static final List<SupportedFileFormat> supportedFileFormat;

    static {
        Builder<SupportedFileFormat> supportedFileFormatBuilder = ImmutableList.builder();
        supportedFileFormatBuilder.add(new TextFileFormat());
        supportedFileFormatBuilder.add(new HFileFileFormat());
        supportedFileFormatBuilder.add(new ParquetFileFormat());
        supportedFileFormat = supportedFileFormatBuilder.build();
    }

    public static List<SupportedFileFormat> supportedFileFormat() {
        // SupportedFileFormat.
        return supportedFileFormat;
    }

    public static SupportedFileFormat getSupportedFileFormat(String fileFormat) {
        if (StringUtils.isEmpty(fileFormat)) {
            throw new IllegalArgumentException("param fileFormat can not be empty");
        }
        for (SupportedFileFormat format : supportedFileFormat) {
            if (StringUtils.equals(format.identityValue(), fileFormat)) {
                return format;
            }
        }
        throw new IllegalArgumentException("illegal fileFormat:" + fileFormat);
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

        @Override
        public FileFormat createFileFormatReader(String entityName, List<HiveTabColType> cols
                , AbstractSerDe serde, Class<?> inputFormatClass, JobConf jobConf) throws Exception {
            org.apache.hadoop.mapred.TextInputFormat inputFormat
                    = (org.apache.hadoop.mapred.TextInputFormat) inputFormatClass.getDeclaredConstructor().newInstance();
            inputFormat.configure(jobConf);
            return new HadoopTextInputFormat(entityName, cols.size(), inputFormat, serde, jobConf);
        }

        @Override
        public String identityValue() {
            return "Text";
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
                String entityName, List<HiveTabColType> cols, AbstractSerDe serde, Class<?> inputFormatClass, JobConf jobConf) throws Exception {
            HiveHBaseTableInputFormat hfileInputFormat
                    = (HiveHBaseTableInputFormat) inputFormatClass.getDeclaredConstructor().newInstance();
            return new HadoopHFileInputFormat(entityName, cols.size(), hfileInputFormat, serde, jobConf);
        }

        @Override
        public String identityValue() {
            return "HFile";
        }
    }

    private static class ParquetFileFormat extends SupportedFileFormat {
        @Override
        public boolean match(Class<?> inputFormatClass) {
            return MapredParquetInputFormat.class == inputFormatClass;
        }

        @Override
        public FileFormat createFileFormatReader(
                String entityName, List<HiveTabColType> cols, AbstractSerDe serde, Class<?> inputFormatClass, JobConf jobConf) throws Exception {
            MapredParquetInputFormat pqInputFormat = (MapredParquetInputFormat) inputFormatClass.getDeclaredConstructor().newInstance();
            return new HadoopParquetInputFormat(entityName, cols.size(), pqInputFormat, serde, jobConf);
        }

        @Override
        public String identityValue() {
            return "Parquet";
        }
    }
}
