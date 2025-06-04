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

package org.apache.flink.kubernetes.entrypoint;

import com.google.common.collect.Lists;
import com.qlangtech.plugins.incr.flink.TISFlinkClassLoaderFactory;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.manage.common.incr.StreamContextConstant.TISRes;
import com.qlangtech.tis.manage.common.incr.UberJarUtil;
import org.apache.flink.client.cli.ArtifactFetchOptions;
import org.apache.flink.client.deployment.application.ApplicationClusterEntryPoint;
import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.client.program.DefaultPackagedProgramRetriever;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.client.program.PackagedProgramRetriever;
import org.apache.flink.client.program.PackagedProgramUtils;
import org.apache.flink.client.program.artifact.ArtifactFetchManager;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.plugin.PluginManager;
import org.apache.flink.core.plugin.PluginUtils;
import org.apache.flink.kubernetes.configuration.KubernetesConfigOptions;
import org.apache.flink.runtime.entrypoint.ClusterEntrypoint;
import org.apache.flink.runtime.entrypoint.ClusterEntrypointUtils;
import org.apache.flink.runtime.entrypoint.DynamicParametersConfigurationParserFactory;
import org.apache.flink.runtime.security.contexts.SecurityContext;
import org.apache.flink.runtime.util.EnvironmentInformation;
import org.apache.flink.runtime.util.JvmShutdownSafeguard;
import org.apache.flink.runtime.util.SignalHandler;
import org.apache.flink.util.FlinkException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.net.URL;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * 当客户端选择使用 kerbernetes-application 部署方式的时候，在flink-jobManager 端服务组装构建jobGraph实例，需要从TIS-console端拉取 同步任务对应的jar包到本地
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-01-08 15:47
 * // @see KubernetesApplicationClusterEntrypoint
 **/
public class KubernetesApplicationClusterEntrypointOfTIS extends ApplicationClusterEntryPoint {
    protected static final Logger logger = LoggerFactory.getLogger(KubernetesApplicationClusterEntrypointOfTIS.class);
    //  private static final PluginAndCfgSnapshotLocalCache snapshotLocalCache = new PluginAndCfgSnapshotLocalCache();

    private KubernetesApplicationClusterEntrypointOfTIS(Configuration configuration, PackagedProgram program) {
        super(configuration, program, KubernetesResourceManagerFactory.getInstance());
    }


    public static void main(final String[] args) {
        // startup checks and logging
        EnvironmentInformation.logEnvironmentInfo(
                LOG, KubernetesApplicationClusterEntrypoint.class.getSimpleName(), args);
        SignalHandler.register(LOG);
        JvmShutdownSafeguard.installAsShutdownHook(LOG);

        final Configuration dynamicParameters =
                ClusterEntrypointUtils.parseParametersOrExit(
                        args,
                        new DynamicParametersConfigurationParserFactory(),
                        KubernetesApplicationClusterEntrypoint.class);
        final Configuration configuration =
                KubernetesEntrypointUtils.loadConfiguration(dynamicParameters);

        List<String> appArgs = configuration.get(ApplicationConfiguration.APPLICATION_ARGS);
        TargetResName targetResName = null;
        for (String collectionName : appArgs) {
            targetResName = new TargetResName(collectionName);
            break;
        }
        Objects.requireNonNull(targetResName, "targetResName can not be null");

        PackagedProgram program = null;
        try {

            TISRes unberJarFile = UberJarUtil.getStreamUberJarFile(targetResName);
            unberJarFile.sync2Local(true);
            String unberJarURL = "local" + String.valueOf(unberJarFile.getFile().toURI().toURL()).substring("file".length());
            logger.info("TIS unberJarURL:{}", unberJarURL);
            TISFlinkClassLoaderFactory.synAppRelevantCfgsAndTpis(new URL[]{unberJarFile.getFile().toURI().toURL()});
            configuration.set(PipelineOptions.JARS, Lists.newArrayList(unberJarURL));

            PluginManager pluginManager =
                    PluginUtils.createPluginManagerFromRootFolder(configuration);
            LOG.info(
                    "Install default filesystem for fetching user artifacts in Kubernetes Application Mode.");
            FileSystem.initialize(configuration, pluginManager);
            SecurityContext securityContext = installSecurityContext(configuration);
            program = securityContext.runSecured(() -> getPackagedProgram(configuration));
        } catch (Exception e) {
            LOG.error("Could not create application program.", e);
            System.exit(1);
        }

        try {
            configureExecution(configuration, program);
        } catch (Exception e) {
            LOG.error("Could not apply application configuration.", e);
            System.exit(1);
        }

        final KubernetesApplicationClusterEntrypointOfTIS kubernetesApplicationClusterEntrypoint =
                new KubernetesApplicationClusterEntrypointOfTIS(configuration, program);

        ClusterEntrypoint.runClusterEntrypoint(kubernetesApplicationClusterEntrypoint);
    }

    private static PackagedProgram getPackagedProgram(final Configuration configuration)
            throws FlinkException {

        final ApplicationConfiguration applicationConfiguration =
                ApplicationConfiguration.fromConfiguration(configuration);

        final PackagedProgramRetriever programRetriever =
                getPackagedProgramRetriever(
                        configuration,
                        applicationConfiguration.getProgramArguments(),
                        applicationConfiguration.getApplicationClassName());
        return programRetriever.getPackagedProgram();
    }

    private static PackagedProgramRetriever getPackagedProgramRetriever(
            final Configuration configuration,
            final String[] programArguments,
            @Nullable final String jobClassName)
            throws FlinkException {

        final File userLibDir = ClusterEntrypointUtils.tryFindUserLibDirectory().orElse(null);

        // No need to do pipelineJars validation if it is a PyFlink job.
        if (!(PackagedProgramUtils.isPython(jobClassName)
                || PackagedProgramUtils.isPython(programArguments))) {
            final ArtifactFetchManager.Result fetchRes = fetchArtifacts(configuration);

            return DefaultPackagedProgramRetriever.create(
                    userLibDir,
                    fetchRes.getJobJar(),
                    fetchRes.getArtifacts(),
                    jobClassName,
                    programArguments,
                    configuration);
        }

        return DefaultPackagedProgramRetriever.create(
                userLibDir, jobClassName, programArguments, configuration);
    }

    private static ArtifactFetchManager.Result fetchArtifacts(Configuration configuration) {
        try {
            String targetDir = generateJarDir(configuration);
            ArtifactFetchManager fetchMgr = new ArtifactFetchManager(configuration, targetDir);

            List<String> uris = configuration.get(PipelineOptions.JARS);
            checkArgument(uris.size() == 1, "Should only have one jar");
            List<String> additionalUris =
                    configuration
                            .getOptional(ArtifactFetchOptions.ARTIFACT_LIST)
                            .orElse(Collections.emptyList());

            return fetchMgr.fetchArtifacts(uris.get(0), additionalUris);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    static String generateJarDir(Configuration configuration) {
        return String.join(
                File.separator,
                new File(configuration.get(ArtifactFetchOptions.BASE_DIR)).getAbsolutePath(),
                configuration.get(KubernetesConfigOptions.NAMESPACE),
                configuration.get(KubernetesConfigOptions.CLUSTER_ID));
    }


}
