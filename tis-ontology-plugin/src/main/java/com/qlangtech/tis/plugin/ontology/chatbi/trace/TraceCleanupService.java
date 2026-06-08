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
import com.qlangtech.tis.plugin.ontology.chatbi.config.TraceConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

/**
 * Trace 清理服务（单例）。
 * <p>
 * 负责按时间和数量清理历史 trace 文件。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/4
 */
public class TraceCleanupService {

    private static final Logger logger = LoggerFactory.getLogger(TraceCleanupService.class);
    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private static volatile TraceCleanupService INSTANCE;

    private final ScheduledExecutorService cleanupExecutor;
    private final Map<String, Long> lastCleanupTimeByDomain = new ConcurrentHashMap<>();

    private TraceConfig traceConfig;

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

    public void setConfig(TraceConfig config) {
        this.traceConfig = config;
    }

    /**
     * 触发清理（写入 trace 时调用）
     */
    public void triggerCleanup(String domain) {
        if (traceConfig == null || !traceConfig.isEnableAutoCleanup()) {
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
                performCleanup(domain);
            } catch (Exception e) {
                logger.error("Failed to cleanup trace for domain: " + domain, e);
            }
        });
    }

    /**
     * 执行清理
     */
    private void performCleanup(String domain) {
        if (traceConfig == null) {
            return;
        }

        File traceBaseDir = new File(Config.getDataDir(), "chatbi/trace");
        if (!traceBaseDir.exists()) {
            return;
        }

        int retentionDays = traceConfig.getRetentionDays();
        int maxTracesPerDomain = traceConfig.getMaxTracesPerDomain();

        LocalDate cutoffDate = LocalDate.now().minusDays(retentionDays);

        // Step 1: 删除过期的日期目录
        File[] dateDirs = traceBaseDir.listFiles();
        if (dateDirs == null) {
            return;
        }

        int deletedDirs = 0;
        for (File dateDir : dateDirs) {
            if (!dateDir.isDirectory()) {
                continue;
            }

            try {
                LocalDate dirDate = LocalDate.parse(dateDir.getName(), DATE_FORMATTER);
                if (dirDate.isBefore(cutoffDate)) {
                    deleteDirectory(dateDir);
                    deletedDirs++;
                }
            } catch (Exception e) {
                logger.warn("Failed to parse date directory: " + dateDir.getName(), e);
            }
        }

        // Step 2: 按 domain 限制文件数量
        List<File> allTraceFiles = new ArrayList<>();
        for (File dateDir : dateDirs) {
            if (!dateDir.isDirectory() || !dateDir.exists()) {
                continue;
            }

            File[] traceFiles = dateDir.listFiles(f -> f.getName().endsWith(".jsonl"));
            if (traceFiles != null) {
                allTraceFiles.addAll(Arrays.asList(traceFiles));
            }
        }

        // 按 domain 分组（从文件内容读取 domain 信息，这里简化为按文件名）
        // 实际实现可能需要读取文件第一行的 domain 字段
        // 这里简化为保留最新的 maxTracesPerDomain 个文件
        if (allTraceFiles.size() > maxTracesPerDomain) {
            allTraceFiles.sort(Comparator.comparingLong(File::lastModified).reversed());
            int deletedFiles = 0;
            for (int i = maxTracesPerDomain; i < allTraceFiles.size(); i++) {
                if (allTraceFiles.get(i).delete()) {
                    deletedFiles++;
                }
            }

            if (deletedFiles > 0) {
                logger.info("Cleaned up {} old trace files for domain: {}", deletedFiles, domain);
            }
        }

        if (deletedDirs > 0) {
            logger.info("Cleaned up {} expired date directories", deletedDirs);
        }
    }

    private void deleteDirectory(File dir) {
        File[] files = dir.listFiles();
        if (files != null) {
            for (File file : files) {
                if (file.isDirectory()) {
                    deleteDirectory(file);
                } else {
                    file.delete();
                }
            }
        }
        dir.delete();
        logger.debug("Deleted trace directory: {}", dir.getAbsolutePath());
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
