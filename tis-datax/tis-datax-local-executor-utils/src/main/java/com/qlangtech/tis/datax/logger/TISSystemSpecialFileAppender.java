///**
// *   Licensed to the Apache Software Foundation (ASF) under one
// *   or more contributor license agreements.  See the NOTICE file
// *   distributed with this work for additional information
// *   regarding copyright ownership.  The ASF licenses this file
// *   to you under the Apache License, Version 2.0 (the
// *   "License"); you may not use this file except in compliance
// *   with the License.  You may obtain a copy of the License at
// *
// *       http://www.apache.org/licenses/LICENSE-2.0
// *
// *   Unless required by applicable law or agreed to in writing, software
// *   distributed under the License is distributed on an "AS IS" BASIS,
// *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// *   See the License for the specific language governing permissions and
// *   limitations under the License.
// */
//
//package com.qlangtech.tis.datax.logger;
//
//import ch.qos.logback.classic.spi.ILoggingEvent;
//import ch.qos.logback.core.FileAppender;
//
//import java.io.File;
//
///**
// * 支持在Dolphinscheduler task 运行时设置taskpath属性运行
// * https://logback.qos.ch/manual/appenders.html#FileAppender
// * @author: 百岁（baisui@qlangtech.com）
// * @create: 2024-09-01 21:50
// **/
//public class TISSystemSpecialFileAppender extends FileAppender<ILoggingEvent> {
////    private static File targetLoggerFile;
////
////    public static void setTargetLoggerFile(File file) {
////        targetLoggerFile = file;
////    }
//
//    @Override
//    public void start() {
//        int errors = 0;
//        if (targetLoggerFile != null) {
//            if (!targetLoggerFile.exists()) {
//                addError("Collisions detected with FileAppender/RollingAppender instances defined earlier. Aborting.");
//                errors++;
//            } else {
//                this.setFile(targetLoggerFile.getAbsolutePath());
//            }
//
//        } else {
//            errors++;
//            addError("\"targetLoggerFile\" property not set for appender ");
//        }
//        if (errors == 0) {
//            super.start();
//        }
//    }
//}
