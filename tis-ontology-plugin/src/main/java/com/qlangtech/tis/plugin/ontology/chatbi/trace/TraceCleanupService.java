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

import com.qlangtech.tis.manage.common.Config;
import com.qlangtech.tis.plugin.ontology.EnableChatBI;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * Trace 清理服务（单例）。
 * <p>
 * 负责按时间和数量清理历史 trace 文件。
 * <p>
 * trace 文件命名格式：{@code yyyyMMddHHmmss-{uuid32}.jsonl}，
 * 直接从文件名前14位解析创建时间，无需日期子目录。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/4
 */
public class TraceCleanupService {

    private static final Logger logger = LoggerFactory.getLogger(TraceCleanupService.class);

    /** 文件名前14位：yyyyMMddHHmmss */
    private static final DateTimeFormatter DATETIME_FORMATTER =
            DateTimeFormatter.ofPattern("yyyyMMddHHmmss");

    private static volatile TraceCleanupService INSTANCE;

    private final ScheduledExecutorService cleanupExecutor;
    private final Map<String, Long> lastCleanupTimeByDomain = new ConcurrentHashMap<>();

    private TraceCleanupService() {
        this.cleanupExecutor = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "trace-cleanup");
            t.setDaemon(true);
            return t;
        });
    }

    public static TraceCleanupService getInstance() {
        if (INSTANCE == null) {
            synchronized (TraceCleanupService.class) {
                if (INSTANCE == null) {
                    INSTANCE = new TraceCleanupService();
                }
            }
        }
        return INSTANCE;
    }

    /**
     * 触发清理（写入 trace 时调用）
     */
    public void triggerCleanup(String domain) {
        EnableChatBI chatBI = EnableChatBI.load(domain);

        if (chatBI == null || !chatBI.traceConfig.isEnableAutoCleanup()) {
            return;
        }

        // 频率限制：每个 domain 最多 1 小时触发一次
        Long lastCleanup = lastCleanupTimeByDomain.get(domain);
        long now = System.currentTimeMillis();
        if (lastCleanup != null && (now - lastCleanup) < TimeUnit.HOURS.toMillis(1)) {
            return;
        }

        lastCleanupTimeByDomain.put(domain, now);

        // 异步执行清理
        cleanupExecutor.submit(() -> {
            try {
                performCleanup(chatBI, domain);
            } catch (Exception e) {
                logger.error("Failed to cleanup trace for domain: " + domain, e);
            }
        });
    }

    /**
     * 执行清理：
     * 1. 删除创建时间早于 retentionDays 的文件（从文件名前14位解析）
     * 2. 若文件总数超过 maxTracesPerDomain，按文件名字典序倒序（即时间倒序）保留最新的
     */
    private void performCleanup(EnableChatBI chatBI, String domain) {
        if (chatBI == null) {
            return;
        }

        File domainDir = new File(Config.getDataDir(), "chatbi/trace/" + domain);
        if (!domainDir.exists()) {
            return;
        }

        int retentionDays = chatBI.traceConfig.getRetentionDays();
        int maxTracesPerDomain = chatBI.traceConfig.getMaxTracesPerDomain();

        LocalDateTime cutoffTime = LocalDateTime.now().minusDays(retentionDays);

        File[] traceFiles = domainDir.listFiles(f -> f.isFile() && f.getName().endsWith(".jsonl"));
        if (traceFiles == null || traceFiles.length == 0) {
            return;
        }

        // Step 1: 删除过期文件（文件名前14位为创建时间）
        List<File> validFiles = new ArrayList<>();
        int deletedByAge = 0;
        for (File file : traceFiles) {
            LocalDateTime fileTime = parseCreateTime(file.getName());
            if (fileTime != null && fileTime.isBefore(cutoffTime)) {
                if (file.delete()) {
                    deletedByAge++;
                }
            } else {
                validFiles.add(file);
            }
        }
        if (deletedByAge > 0) {
            logger.info("Cleaned up {} expired trace files for domain: {}", deletedByAge, domain);
        }

        // Step 2: 按文件名字典序倒序（时间倒序），超出 maxTracesPerDomain 的删除
        if (validFiles.size() > maxTracesPerDomain) {
            // 文件名已含时间前缀，字典序 = 时间顺序；倒序后前面是最新的
            validFiles.sort(Comparator.comparing(File::getName).reversed());
            int deletedByCount = 0;
            for (int i = maxTracesPerDomain; i < validFiles.size(); i++) {
                if (validFiles.get(i).delete()) {
                    deletedByCount++;
                }
            }
            if (deletedByCount > 0) {
                logger.info("Cleaned up {} over-limit trace files for domain: {}", deletedByCount, domain);
            }
        }
    }

    /**
     * 从文件名中解析创建时间（前14位 yyyyMMddHHmmss）。
     * 文件名格式：{yyyyMMddHHmmss}-{uuid32}.jsonl
     */
    private LocalDateTime parseCreateTime(String fileName) {
        if (fileName == null || fileName.length() < 14) {
            return null;
        }
        try {
            return LocalDateTime.parse(fileName.substring(0, 14), DATETIME_FORMATTER);
        } catch (DateTimeParseException e) {
            logger.warn("Cannot parse create time from trace file name: {}", fileName);
            return null;
        }
    }

    public void shutdown() {
        cleanupExecutor.shutdown();
        try {
            if (!cleanupExecutor.awaitTermination(5, TimeUnit.SECONDS)) {
                cleanupExecutor.shutdownNow();
            }
        } catch (InterruptedException e) {
            cleanupExecutor.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
