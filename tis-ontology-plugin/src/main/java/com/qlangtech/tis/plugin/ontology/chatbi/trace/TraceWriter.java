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
package com.qlangtech.tis.plugin.ontology.chatbi.trace;

import com.alibaba.fastjson.JSON;
import com.qlangtech.tis.plugin.ontology.chatbi.TraceStep;
import com.qlangtech.tis.manage.common.Config;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.UUID;

/**
 * Trace 日志写入器（§7 T6）。
 * <p>
 * 落盘到 {@code <TIS.dataDir>/chatbi/trace/<yyyy-mm-dd>/<reqId>.jsonl}。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/2
 */
public class TraceWriter {

    private static final Logger logger = LoggerFactory.getLogger(TraceWriter.class);

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    /**
     * 写入完整 trace。
     *
     * @param domain 本体域名
     * @param nlq    自然语言问句
     * @param trace  trace 步骤列表
     * @return trace 文件路径
     */
    public static File writeTrace(String domain, String nlq, List<TraceStep> trace) {
        String reqId = generateRequestId();
        File traceFile = getTraceFile(reqId);

        try {
            traceFile.getParentFile().mkdirs();
            try (PrintWriter writer = new PrintWriter(new FileWriter(traceFile))) {
                // 写入请求头
                writer.println(JSON.toJSONString(Map.of(
                        "reqId", reqId,
                        "domain", domain,
                        "nlq", nlq,
                        "timestamp", System.currentTimeMillis()
                )));

                // 写入每一步 trace
                for (TraceStep step : trace) {
                    writer.println(JSON.toJSONString(step));
                }
            }
            logger.info("Trace written to: {}", traceFile.getAbsolutePath());
            return traceFile;
        } catch (IOException e) {
            logger.error("Failed to write trace file: " + traceFile, e);
            return null;
        }
    }

    private static File getTraceFile(String reqId) {
        File dataDir = Config.getDataDir();
        String today = LocalDate.now().format(DATE_FORMATTER);
        return new File(dataDir, "chatbi/trace/" + today + "/" + reqId + ".jsonl");
    }

    private static String generateRequestId() {
        return UUID.randomUUID().toString().replace("-", "");
    }

    private static class Map {
        static java.util.Map<String, Object> of(String k1, Object v1, String k2, Object v2, String k3, Object v3, String k4, Object v4) {
            java.util.Map<String, Object> map = new java.util.HashMap<>();
            map.put(k1, v1);
            map.put(k2, v2);
            map.put(k3, v3);
            map.put(k4, v4);
            return map;
        }
    }
}
