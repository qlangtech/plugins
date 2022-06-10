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
//package com.qlangtech.tis.org.apache.hudi.keygen;
//
//import java.io.Serializable;
//
///**
// * @author: 百岁（baisui@qlangtech.com）
// * @create: 2022-05-09 15:17
// **/
//public class TimestampBasedAvroKeyGenerator {
//    public enum TimestampType implements Serializable {
//        UNIX_TIMESTAMP, DATE_STRING, MIXED, EPOCHMILLISECONDS, SCALAR
//    }
//    /**
//     * Supported configs.
//     */
//    public static class Config {
//
//        // One value from TimestampType above
//        public static final String TIMESTAMP_TYPE_FIELD_PROP = "hoodie.deltastreamer.keygen.timebased.timestamp.type";
//        public static final String INPUT_TIME_UNIT =
//                "hoodie.deltastreamer.keygen.timebased.timestamp.scalar.time.unit";
//        //This prop can now accept list of input date formats.
//        public static final String TIMESTAMP_INPUT_DATE_FORMAT_PROP =
//                "hoodie.deltastreamer.keygen.timebased.input.dateformat";
//        public static final String TIMESTAMP_INPUT_DATE_FORMAT_LIST_DELIMITER_REGEX_PROP = "hoodie.deltastreamer.keygen.timebased.input.dateformat.list.delimiter.regex";
//        public static final String TIMESTAMP_INPUT_TIMEZONE_FORMAT_PROP = "hoodie.deltastreamer.keygen.timebased.input.timezone";
//        public static final String TIMESTAMP_OUTPUT_DATE_FORMAT_PROP =
//                "hoodie.deltastreamer.keygen.timebased.output.dateformat";
//        //still keeping this prop for backward compatibility so that functionality for existing users does not break.
//        public static final String TIMESTAMP_TIMEZONE_FORMAT_PROP =
//                "hoodie.deltastreamer.keygen.timebased.timezone";
//        public static final String TIMESTAMP_OUTPUT_TIMEZONE_FORMAT_PROP = "hoodie.deltastreamer.keygen.timebased.output.timezone";
//        static final String DATE_TIME_PARSER_PROP = "hoodie.deltastreamer.keygen.datetime.parser.class";
//    }
//}
