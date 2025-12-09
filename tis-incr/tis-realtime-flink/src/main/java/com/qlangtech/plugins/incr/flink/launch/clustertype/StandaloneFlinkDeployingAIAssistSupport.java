package com.qlangtech.plugins.incr.flink.launch.clustertype;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.plugins.incr.flink.common.FlinkCluster;
import com.qlangtech.plugins.incr.flink.launch.FlinkPropAssist;
import com.qlangtech.tis.aiagent.core.IAgentContext;
import com.qlangtech.tis.aiagent.llm.LLMProvider;
import com.qlangtech.tis.config.BasicConfig;
import com.qlangtech.tis.config.flink.IFlinkCluster;
import com.qlangtech.tis.extension.AIAssistSupport;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.model.UpdateCenter;
import com.qlangtech.tis.lang.PayloadLink;
import com.qlangtech.tis.manage.common.CenterResource;
import com.qlangtech.tis.maven.plugins.tpi.ICoord;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.IdentityName;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.realtime.utils.NetUtils;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.util.DescribableJSON;
import com.qlangtech.tis.util.IPluginContext;
import com.qlangtech.tis.util.impl.AttrVals;
import com.qlangtech.tis.util.impl.PluginEqualResult;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.configuration.TaskManagerOptions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.yaml.snakeyaml.DumperOptions;
import org.yaml.snakeyaml.Yaml;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.management.ManagementFactory;
import java.lang.management.OperatingSystemMXBean;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

import static com.qlangtech.plugins.incr.flink.common.FlinkCluster.FLINK_DEFAULT_RETRY_MAX_ATTEMPTS;
import static com.qlangtech.tis.manage.common.Config.KEY_TIS_PLUGIN_ROOT;
import static com.qlangtech.tis.manage.common.Config.SUB_DIR_LIBS;
import static com.qlangtech.tis.realtime.utils.NetUtils.LOCAL_HOST_VALUE;

/**
 * TIS 初次安装 本地环境中还没有部署 Flink Standalone环境，如果用户还没有安装的话，需要Agent先将本地Standalone环境部署完毕
 *
 * <p>需求说明文件：requirment/add-ai-assist-standalone-flink-deployment-processing.md</p>
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2025/12/5
 */
public class StandaloneFlinkDeployingAIAssistSupport extends AIAssistSupport<Standalone> {

    private static final Logger logger = LoggerFactory.getLogger(StandaloneFlinkDeployingAIAssistSupport.class);

    public static final String KEY_FIELD_TM_MEMORY = "tmMemory";
    // public static final String KEY_FIELD_NAME = "name";
    public static final String KEY_FIELD_DATA_DIR = "dataDir";
    public static final String KEY_FIELD_FLINK_DEPLOY_DIR = "flinkDeployDir";
    public static final String KEY_IDENTITY_NAME = "flink_standalone_deploy_ai_assist";
    private static final String FLINK_PACKAGE_NAME = "flink-tis-1.20.1";
    private static final String FLINK_PACKAGE_TAR_NAME = FLINK_PACKAGE_NAME + "-bin.tar.gz";
    private static final long JM_MEMORY_MB = 1600; // JobManager memory in MB
    private static final int STARTUP_WAIT_SECONDS = 5;
    private static final int VERIFY_MAX_RETRIES = 3;
    private static final int VERIFY_RETRY_INTERVAL_SECONDS = 2;

    @FormField(ordinal = 0, identity = true, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
    public String name;
    /**
     * flink 本地部署的目录
     */
    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.absolute_path})
    public String flinkDeployDir;

    @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.absolute_path})
    public String dataDir;

    @FormField(ordinal = 3, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer slot;

    @FormField(ordinal = 4, type = FormFieldType.MEMORY_SIZE_OF_MEGA, validate = {Validator.require})
    public com.qlangtech.tis.plugin.MemorySize tmMemory;

    @FormField(ordinal = 5, type = FormFieldType.INT_NUMBER, advance = true, validate = {Validator.require, Validator.integer})
    public Integer port;


    @Override
    public String getDescription() {
        return "当前节点还没有安装Standalone Flink Cluster，现在开始部署吧";
    }

    @Override
    public boolean environmentSupport() {
        // 通过环境变量判断 , 确定当前是否为linux运行环境
        if (BasicConfig.inDockerContainer()) {
            return false;
        }
        return !SystemUtils.IS_OS_WINDOWS;
    }

    @Override
    public Standalone startProcess(IAgentContext agentContext, IPluginContext pluginContext, Context context) {

        agentContext.sendLLMStatus(LLMProvider.LLMChatPhase.Start, "Starting Flink Standalone deployment process...");
        boolean hasFaild = false;
        try {
            // Step 1: Check if Flink is already running
            if (isFlinkRunning()) {
                logger.info("Flink is already running on port {}, reusing existing cluster", this.port);
                return createNewFlinkCluster(agentContext, pluginContext, context);
            }

            // Step 2: Validate flinkDeployDir
            validateFlinkDeployDir();

            // Step 3: Validate dataDir structure
            validateDataDir();

            // Step 4: Check and deploy Flink if necessary
            File flinkHome = new File(this.flinkDeployDir, FLINK_PACKAGE_NAME);
            if (!flinkHome.exists()) {
                logger.info("Flink not found at {}, starting deployment...", flinkHome.getAbsolutePath());
                downloadAndExtractFlink(flinkHome);
            } else {
                logger.info("Flink already exists at {}, skipping download", flinkHome.getAbsolutePath());
            }

            // Step 5: Validate system memory
            validateSystemMemory();

            // Step 6: Modify Flink configuration
            modifyFlinkConfiguration(flinkHome);

            // Step 7: Start Flink cluster
            startFlinkCluster(flinkHome);

            // Step 8: Verify Flink is running
            verifyFlinkStartup();

            // Step 9: Create and return Standalone instance
            logger.info("Flink Standalone deployment completed successfully");
            return createNewFlinkCluster(agentContext, pluginContext, context);

        } catch (Exception e) {
            hasFaild = true;
            agentContext.sendLLMStatus(LLMProvider.LLMChatPhase.ERROR, e.getMessage());
            throw new RuntimeException("Failed to deploy Flink Standalone cluster", e);
        } finally {
            if (!hasFaild) {
                agentContext.sendLLMStatus(LLMProvider.LLMChatPhase.Complete, null);
            }
        }
    }


    /**
     * Check if Flink is already running
     */
    private boolean isFlinkRunning() {
        return NetUtils.isPortAvailable(LOCAL_HOST_VALUE, this.port);
    }

    /**
     * Validate flinkDeployDir exists and is writable
     */
    private void validateFlinkDeployDir() {
        File deployDir = new File(this.flinkDeployDir);
        if (!deployDir.exists()) {
            throw new IllegalStateException("Flink deploy directory does not exist: " + this.flinkDeployDir);
        }
        if (!deployDir.isDirectory()) {
            throw new IllegalStateException("Flink deploy path is not a directory: " + this.flinkDeployDir);
        }
        if (!deployDir.canWrite()) {
            throw new IllegalStateException("No write permission for Flink deploy directory: " + this.flinkDeployDir);
        }
        logger.info("Validated Flink deploy directory: {}", this.flinkDeployDir);
    }

    /**
     * Validate dataDir has required structure (libs/plugins/*.tpi)
     */
    private void validateDataDir() {
        File dataDir = new File(this.dataDir);
        if (!dataDir.exists() || !dataDir.isDirectory()) {
            throw new IllegalStateException("Data directory does not exist or is not a directory: " + this.dataDir);
        }

        File libsDir = new File(dataDir, SUB_DIR_LIBS);
        if (!libsDir.exists() || !libsDir.isDirectory()) {
            throw new IllegalStateException("libs directory does not exist in dataDir: " + libsDir.getAbsolutePath());
        }

        File pluginsDir = new File(libsDir, KEY_TIS_PLUGIN_ROOT);
        if (!pluginsDir.exists() || !pluginsDir.isDirectory()) {
            throw new IllegalStateException("plugins directory does not exist in libs: " + pluginsDir.getAbsolutePath());
        }

        File[] tpiFiles = pluginsDir.listFiles((dir, name) -> name.endsWith(ICoord.KEY_TPI_FILE_EXTENSION));
        if (tpiFiles == null || tpiFiles.length < 1) {
            throw new IllegalStateException("No .tpi files found in plugins directory: " + pluginsDir.getAbsolutePath());
        }

        logger.info("Validated dataDir structure with {} .tpi files", tpiFiles.length);
    }

    /**
     * Download and extract Flink tarball
     */
    private void downloadAndExtractFlink(File flinkHome) {
        // URL flinkPkgTarUrl = null;
        try {
            //  flinkPkgTarUrl = UpdateCenterResource.getTISTarPkg(FLINK_PACKAGE_TAR_NAME);
            // File tempDirectory = FileUtils.getTempDirectory();
            UpdateCenter.copyTarToLocal(FLINK_PACKAGE_TAR_NAME, flinkHome, Optional.empty());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        File flinBinDir = getFlinBinDir(flinkHome);
        for (File sh : FileUtils.listFiles(flinBinDir, new String[]{"sh"}, true)) {
            if (!sh.setExecutable(true, false)) {
                throw new IllegalStateException("Flink start script is not executable " + sh.getAbsolutePath());
            }
        }
    }

//    /**
//     * Extract tar.gz file to target directory
//     */
//    private void extractTarGz(File tarGzFile, File destDir) throws IOException {
//        try (FileInputStream fis = new FileInputStream(tarGzFile);
//             GzipCompressorInputStream gzis = new GzipCompressorInputStream(fis);
//             TarArchiveInputStream tais = new TarArchiveInputStream(gzis)) {
//
//            TarArchiveEntry entry;
//            while ((entry = tais.getNextTarEntry()) != null) {
//                File outputFile = new File(destDir, entry.getName());
//
//                if (entry.isDirectory()) {
//                    if (!outputFile.exists() && !outputFile.mkdirs()) {
//                        throw new IOException("Failed to create directory: " + outputFile.getAbsolutePath());
//                    }
//                } else {
//                    File parent = outputFile.getParentFile();
//                    if (!parent.exists() && !parent.mkdirs()) {
//                        throw new IOException("Failed to create parent directory: " + parent.getAbsolutePath());
//                    }
//
//                    try (FileOutputStream fos = new FileOutputStream(outputFile)) {
//                        IOUtils.copy(tais, fos);
//                    }
//
//                    // Preserve executable permissions
//                    if ((entry.getMode() & 0100) != 0) {
//                        outputFile.setExecutable(true);
//                    }
//                }
//            }
//        }
//    }

    /**
     * Validate system has enough memory
     */
    private void validateSystemMemory() {


        OperatingSystemMXBean osBean = ManagementFactory.getOperatingSystemMXBean();
        if (osBean instanceof com.sun.management.OperatingSystemMXBean) {
            com.sun.management.OperatingSystemMXBean sunOsBean = (com.sun.management.OperatingSystemMXBean) osBean;
            long totalPhysicalMemoryMB = sunOsBean.getTotalPhysicalMemorySize() / (1024 * 1024);
            long requiredMemoryMB = (getTaskmanagerMemory()) + JM_MEMORY_MB;
            long availableMemoryMB = (long) (totalPhysicalMemoryMB * 0.8);

            logger.info("System memory - Total: {}MB, Required: {}MB (TM: {}MB + JM: {}MB), Available(80%): {}MB",
                    totalPhysicalMemoryMB, requiredMemoryMB,
                    this.tmMemory.getBytes() / (1024 * 1024), JM_MEMORY_MB, availableMemoryMB);

            if (requiredMemoryMB > availableMemoryMB) {
                throw new IllegalStateException(String.format(
                        "Insufficient system memory. Required: %dMB, Available: %dMB (80%% of %dMB total)",
                        requiredMemoryMB, availableMemoryMB, totalPhysicalMemoryMB));
            }
        } else {
            logger.warn("Cannot determine system memory, skipping memory validation");
        }
    }

    private int getTaskmanagerMemory() {
        return (this.tmMemory.getMebiBytes()) * this.slot;
    }

    /**
     * Modify Flink config.yaml with required settings (idempotent)
     */
    private void modifyFlinkConfiguration(File flinkHome) {
        File configFile = new File(flinkHome, "conf/config.yaml");
        if (!configFile.exists()) {
            throw new IllegalStateException("Flink config file not found: " + configFile.getAbsolutePath());
        }

        try {
            // Read YAML
            Yaml yaml = new Yaml();
            Map<String, Object> config;
            try (FileInputStream fis = new FileInputStream(configFile)) {
                config = yaml.load(fis);
                if (config == null) {
                    config = new LinkedHashMap<>();
                }
            }

            // Modify configurations (idempotent)
            config.put("taskmanager.numberOfTaskSlots", this.slot);
            config.put("taskmanager.memory.process.size", (this.getTaskmanagerMemory()) + "m");
            config.put("rest.port", this.port);

            // Modify env.java.opts.all
            String[] javaOpts = new String[]{"env", "java", "opts", "all"};
            String existingJavaOpts = (String) getNestedValue(config, javaOpts);
            String dataDirOpt = " -Ddata.dir=" + this.dataDir;
            String notFetchOpt = " -D" + CenterResource.KEY_notFetchFromCenterRepository + "=true";

            String newJavaOpts;
            if (StringUtils.isEmpty(existingJavaOpts)) {
                newJavaOpts = dataDirOpt.trim() + notFetchOpt;
            } else {
                // Remove existing options if present (idempotent)
                String tempOpts = existingJavaOpts.replaceAll("\\s*-Ddata\\.dir=[^\\s]+", "");
                tempOpts = tempOpts.replaceAll("\\s*-D" + CenterResource.KEY_notFetchFromCenterRepository + "=[^\\s]+", "");
                newJavaOpts = tempOpts + dataDirOpt + notFetchOpt;
            }

            setNestedValue(config, newJavaOpts, javaOpts);

            // Write back YAML
            DumperOptions options = new DumperOptions();
            options.setDefaultFlowStyle(DumperOptions.FlowStyle.BLOCK);
            options.setPrettyFlow(true);
            Yaml yamlWriter = new Yaml(options);

            try (FileWriter writer = new FileWriter(configFile)) {
                yamlWriter.dump(config, writer);
            }

            logger.info("Modified Flink configuration: {}", configFile.getAbsolutePath());

        } catch (IOException e) {
            throw new RuntimeException("Failed to modify Flink configuration file", e);
        }
    }

    /**
     * Get nested value from map
     */
    @SuppressWarnings("unchecked")
    private Object getNestedValue(Map<String, Object> map, String... keys) {
        Map<String, Object> current = map;
        for (int i = 0; i < keys.length - 1; i++) {
            Object value = current.get(keys[i]);
            if (value instanceof Map) {
                current = (Map<String, Object>) value;
            } else {
                return null;
            }
        }
        return current.get(keys[keys.length - 1]);
    }

    /**
     * Set nested value in map
     */
    @SuppressWarnings("unchecked")
    private void setNestedValue(Map<String, Object> map, Object value, String... keys) {
        Map<String, Object> current = map;
        for (int i = 0; i < keys.length - 1; i++) {
            Object obj = current.get(keys[i]);
            if (!(obj instanceof Map)) {
                Map<String, Object> newMap = new LinkedHashMap<>();
                current.put(keys[i], newMap);
                current = newMap;
            } else {
                current = (Map<String, Object>) obj;
            }
        }
        current.put(keys[keys.length - 1], value);
    }

    private File getFlinBinDir(File flinkHome) {
        return new File(flinkHome, "bin");
    }

    /**
     * Start Flink cluster using start-cluster.sh
     */
    private void startFlinkCluster(File flinkHome) {
        File startScript = new File(getFlinBinDir(flinkHome), "start-cluster.sh");
        if (!startScript.exists()) {
            throw new IllegalStateException("Flink start script not found: " + startScript.getAbsolutePath());
        }

        try {
            ProcessBuilder pb = new ProcessBuilder(startScript.getAbsolutePath());
            pb.directory(startScript.getParentFile());
            pb.redirectErrorStream(true);

            logger.info("Starting Flink cluster: {}", startScript.getAbsolutePath());
            Process process = pb.start();

            // Capture output
            StringBuilder output = new StringBuilder();
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(process.getInputStream()))) {
                String line;
                while ((line = reader.readLine()) != null) {
                    output.append(line).append("\n");
                    logger.debug("Flink startup: {}", line);
                }
            }

            int exitCode = process.waitFor();
            if (exitCode != 0) {
                throw new RuntimeException("Flink start script failed with exit code " + exitCode + "\nOutput:\n" + output);
            }

            logger.info("Flink start script completed, waiting {}s for startup...", STARTUP_WAIT_SECONDS);
            TimeUnit.SECONDS.sleep(STARTUP_WAIT_SECONDS);

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new RuntimeException("Flink startup was interrupted", e);
        } catch (IOException e) {
            throw new RuntimeException("Failed to start Flink cluster", e);
        }
    }

    /**
     * Verify Flink is running by checking REST API
     */
    private void verifyFlinkStartup() {
        for (int i = 0; i < VERIFY_MAX_RETRIES; i++) {
            if (isFlinkRunning()) {
                logger.info("Flink cluster verified running on port {}", this.port);
                return;
            }

            if (i < VERIFY_MAX_RETRIES - 1) {
                logger.warn("Flink not yet available, retrying in {}s... ({}/{})",
                        VERIFY_RETRY_INTERVAL_SECONDS, i + 1, VERIFY_MAX_RETRIES);
                try {
                    TimeUnit.SECONDS.sleep(VERIFY_RETRY_INTERVAL_SECONDS);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new RuntimeException("Flink startup verification was interrupted", e);
                }
            }
        }

        throw new RuntimeException("Flink cluster failed to start after " + VERIFY_MAX_RETRIES + " verification attempts");
    }

    /**
     * 创建新的Standalone部署方式的实例
     *
     * @param pluginContext
     * @param context
     * @return
     */
    private Standalone createNewFlinkCluster(IAgentContext agentContext, IPluginContext pluginContext, Context context) {
        IPluginStore<FlinkCluster> flinkClusterStore = FlinkCluster.getFlinkClusterStore();
        List<FlinkCluster> clusters = new ArrayList<>(flinkClusterStore.getPlugins());

        FlinkCluster flinkCluster = new FlinkCluster();
        IdentityName newClusterId = IdentityName.createNewPrimaryFieldValue(IFlinkCluster.KEY_DISPLAY_NAME, clusters);
        flinkCluster.name = newClusterId.identityValue();
        flinkCluster.jobManagerAddress = LOCAL_HOST_VALUE + ":" + this.port;
        flinkCluster.maxRetry = FLINK_DEFAULT_RETRY_MAX_ATTEMPTS;
        flinkCluster.retryDelay = RestOptions.RETRY_DELAY.defaultValue().toMillis();

        AttrVals attrVals = new DescribableJSON(flinkCluster).getPostAttribute().getAttrVals();
        PluginEqualResult isEqual = null;
        // FlinkCluster findCluster = null;
        for (FlinkCluster existCluster : clusters) {
            isEqual = attrVals.isPluginEqual(existCluster);
            if (isEqual.isEqual()) {
                flinkCluster = existCluster;
                break;
            }
        }

        // 如果不存在
        if (isEqual == null || !isEqual.isEqual()) {
            // 保存新的实例
            List<Descriptor.ParseDescribable<FlinkCluster>> dlist = Lists.newArrayList();
            clusters.add(flinkCluster);
            for (FlinkCluster cluster : clusters) {
                dlist.add(new Descriptor.ParseDescribable<>(cluster));
            }
            flinkClusterStore.setPlugins(pluginContext, Optional.of(context), dlist);
        }

        agentContext.sendMessage("Flink Standalone Cluster：'" + flinkCluster.name + "'已经创建成功", new PayloadLink("打开",
                flinkCluster.getJobManagerAddress().getUrl()));

        Standalone standalone = new Standalone();
        standalone.flinkCluster = flinkCluster.name;
        return standalone;
    }

    @Override
    public StandaloneFlinkDeployingAIAssistSupport createConfigInstance() {
        return this;
    }

    @Override
    public String identityValue() {
        return name;
    }

    @TISExtension
    public static class DefaultDesc extends BasicParamsConfigDescriptor {
        protected final FlinkPropAssist.Options<StandaloneFlinkDeployingAIAssistSupport> opts;


        public DefaultDesc() {
            super(KEY_IDENTITY_NAME);
            this.opts = FlinkPropAssist.createOpts(this);
            opts.add("tmMemory", FlinkPropAssist.TISFlinkProp.create(TaskManagerOptions.TOTAL_PROCESS_MEMORY)
                            .overwriteDft((MemorySize.ofMebiBytes(1728)))
                    , (fm) -> {
                        if (fm.tmMemory == null) {
                            return null;
                        }
                        return new MemorySize(fm.tmMemory.getBytes());
                    }
            );
        }

        @Override
        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            return this.validateAll(msgHandler, context, postFormVals);
        }
// com.qlangtech.plugins.incr.flink.launch.clustertype.StandaloneFlinkDeployingAIAssistSupport.KEY_IDENTITY_NAME

        public boolean validateName(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            if (!KEY_IDENTITY_NAME.equals(value)) {
                msgHandler.addFieldError(context, fieldName, "必须为：" + KEY_IDENTITY_NAME);
                return false;
            }
            return true;
        }

        public boolean validateSlot(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
            final int minSlot = 1;
            final int maxSlot = 5;
            if (Integer.parseInt(value) < minSlot) {
                msgHandler.addFieldError(context, fieldName, "不能小于" + minSlot);
                return false;
            }
            if (Integer.parseInt(value) > maxSlot) {
                msgHandler.addFieldError(context, fieldName, "不能大于" + maxSlot);
                return false;
            }
            return true;
        }


//        public boolean validatePort(IFieldErrorHandler msgHandler, Context context, String fieldName, String value) {
//            if (NetUtils.isPortAvailable(LOCAL_HOST_VALUE, Integer.parseInt(value))) {
//                msgHandler.addFieldError(context, fieldName, "端口：" + value + "不可用，或者已被占用");
//                return false;
//            }
//            return true;
//        }

        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {

            StandaloneFlinkDeployingAIAssistSupport aiAssist = postFormVals.newInstance();
            try {
                aiAssist.validateSystemMemory();
            } catch (Exception e) {
                msgHandler.addFieldError(context, KEY_FIELD_TM_MEMORY, e.getMessage());
                return false;
            }

            try {
                aiAssist.validateDataDir();
            } catch (Exception e) {
                msgHandler.addFieldError(context, KEY_FIELD_DATA_DIR, e.getMessage());
                return false;
            }

            try {
                aiAssist.validateFlinkDeployDir();
            } catch (Exception e) {
                msgHandler.addFieldError(context, KEY_FIELD_FLINK_DEPLOY_DIR, e.getMessage());
                return false;
            }

            return true;
        }

        @Override
        public String getDisplayName() {
            return "Deploy Local Flink";
        }
    }
}
