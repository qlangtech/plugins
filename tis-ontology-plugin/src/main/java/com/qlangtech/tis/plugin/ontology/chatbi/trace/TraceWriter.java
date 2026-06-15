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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Trace 日志写入器（§7 T6）。
 * <p>
 * 落盘到 {@code <TIS.dataDir>/chatbi/trace/<domain>/<yyyyMMddHHmmss>-<reqId>.jsonl}。
 * 文件名前缀携带创建时间，同一域下按文件名字典序即为时间顺序，无需日期子目录。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/2
 */
public class TraceWriter {

    private static final Logger logger = LoggerFactory.getLogger(TraceWriter.class);

    /**
     * 写入完整 trace。
     *
     * @param domain 本体域名
     * @param nlq    自然语言问句
     * @param trace  trace 步骤列表
     * @param reqId  请求 ID，格式为 yyyyMMddHHmmss-{uuid32}，由 ask() 开头生成
     * @return trace 文件路径
     */
    public static File writeTrace(String domain, String nlq, List<TraceStep> trace, String reqId) {
        File traceFile = getTraceFile(domain, reqId);

        try {
            traceFile.getParentFile().mkdirs();
            try (PrintWriter writer = new PrintWriter(new FileWriter(traceFile))) {
                // 写入请求头
                Map<String, Object> header = new HashMap<>();
                header.put("reqId", reqId);
                header.put("domain", domain);
                header.put("nlq", nlq);
                header.put("timestamp", System.currentTimeMillis());
                writer.println(JSON.toJSONString(header));

                // 写入每一步 trace
                for (TraceStep step : trace) {
                    writer.println(JSON.toJSONString(step));
                }
            }
            logger.info("Trace written to: {}", traceFile.getAbsolutePath());

            // 触发清理
            TraceCleanupService.getInstance().triggerCleanup(domain);

            return traceFile;
        } catch (IOException e) {
            logger.error("Failed to write trace file: " + traceFile, e);
            return null;
        }
    }

    /**
     * 路径：{dataDir}/chatbi/trace/{domain}/{reqId}.jsonl
     * reqId 已携带 yyyyMMddHHmmss 前缀，文件系统字典序即时间顺序。
     */
    static File getTraceFile(String domain, String reqId) {
        File dataDir = Config.getDataDir();
        return new File(dataDir, "chatbi/trace/" + domain + "/" + reqId + ".jsonl");
    }
}
