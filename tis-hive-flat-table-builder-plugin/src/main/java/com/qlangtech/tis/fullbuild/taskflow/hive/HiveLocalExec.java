/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 *   This program is free software: you can use, redistribute, and/or modify
 *   it under the terms of the GNU Affero General Public License, version 3
 *   or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 *  This program is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *   FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package com.qlangtech.tis.fullbuild.taskflow.hive;

/* *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2016年2月24日
 */
public class HiveLocalExec {

    /**
     * @param args
     */
    public static void main(String[] args) throws Exception {
    // String line = "AcroRd32.exe /p /h " + file.getAbsolutePath();
    // CommandLine cmdLine = new CommandLine("hive");
    // cmdLine.addArgument("--database tis");
    // cmdLine.addArgument("-e");
    // cmdLine.addArgument(
    // "select count(1) from totalpay where pt='20160224001002';",
    // true);
    // 
    // System.out.println("start===================");
    // 
    // // CommandLine cmdLine = new CommandLine("/bin/sh");
    // // cmdLine.addArgument("./dumpcenter-daily.sh");
    // 
    // CommandLine cmdLine = new CommandLine("hive");
    // cmdLine.addArgument("--database");
    // cmdLine.addArgument("tis");
    // cmdLine.addArgument("-e");
    // cmdLine.addArgument(
    // "select count(1) from instance;\n select count(1) from totalpay;",
    // true);
    // 
    // System.out.println("getExecutable:" + cmdLine.getExecutable());
    // System.out.println(cmdLine.getArguments());
    // System.out.println("==============");
    // DefaultExecuteResultHandler resultHandler = new DefaultExecuteResultHandler();
    // 
    // ExecuteWatchdog watchdog = new ExecuteWatchdog(60000);
    // DefaultExecutor executor = new DefaultExecutor();
    // executor.setWorkingDirectory(new File("/home/baisui/tis"));
    // 
    // executor.setStreamHandler(new PumpStreamHandler(System.out));
    // executor.setExitValue(1);
    // executor.setWatchdog(watchdog);
    // executor.execute(cmdLine, resultHandler);
    // 
    // // 等待5个小时
    // resultHandler.waitFor(5 * 60 * 60 * 1000);
    // System.out.println("exec over===================");
    // 
    // System.out.println("exitCode:" + resultHandler.getExitValue());
    // if (resultHandler.getException() != null) {
    // resultHandler.getException().printStackTrace();
    // }
    }
}
