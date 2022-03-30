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

package com.qlangtech.plugins.incr.flink;

import com.google.common.collect.Lists;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.async.message.client.consumer.impl.MQListenerFactory;
import com.qlangtech.tis.extension.PluginManager;
import com.qlangtech.tis.manage.common.CenterResource;
import com.qlangtech.tis.manage.common.Config;
import com.qlangtech.tis.plugin.ComponentMeta;
import com.qlangtech.tis.plugin.IRepositoryResource;
import com.qlangtech.tis.plugin.KeyedPluginStore;
import com.qlangtech.tis.plugin.incr.IncrStreamFactory;
import com.qlangtech.tis.plugin.incr.TISSinkFactory;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager;
import org.apache.flink.runtime.execution.librarycache.ClassLoaderFactoryBuilder;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;
import java.util.stream.Collectors;

import static org.apache.flink.util.FlinkUserCodeClassLoader.NOOP_EXCEPTION_HANDLER;


public class TISFlinClassLoaderFactory implements ClassLoaderFactoryBuilder {

    public static final String SKIP_CLASSLOADER_FACTORY_CREATION = "skip_classloader_factory_creation";


    private static final Logger logger = LoggerFactory.getLogger(TISFlinClassLoaderFactory.class);

//    public static void main(String[] args) throws Exception {
//        File f = new File("/opt/data/tis/cfg_repo/streamscript/mysql_elastic/20210629113249/mysql_elastic-inc.jar");
//        try (JarInputStream jarReader = new JarInputStream(FileUtils.openInputStream(f))) {
//            Manifest manifest = jarReader.getManifest();
//            Attributes pluginInventory = manifest.getAttributes("plugin_inventory");
//            if (pluginInventory == null) {
//                throw new IllegalStateException("plugin inventory can not be empty in lib:" + f);
//            }
//            for (Map.Entry<Object, Object> pluginDesc : pluginInventory.entrySet()) {
//                //  pluginManager.dynamicLoadPlugin(String.valueOf(pluginDesc.getKey()));
//            }
//        }
//    }

    public static void main(String[] args) throws Exception {

    }

    @Override
    public BlobLibraryCacheManager.ClassLoaderFactory buildClientLoaderFactory(
            FlinkUserCodeClassLoaders.ResolveOrder classLoaderResolveOrder
            , String[] alwaysParentFirstPatterns
            , @Nullable Consumer<Throwable> exceptionHander, boolean checkClassLoaderLeak) {
        this.makeDataDirUseable();

        PluginManager pluginManager = TIS.get().getPluginManager();


        return (libraryURLs) -> {
            logger.info("create Flink app classloader:{},resolveOrder:{}"
                    , Arrays.stream(libraryURLs).map((url) -> String.valueOf(url)).collect(Collectors.joining(","))
                    , classLoaderResolveOrder);
            try {
                //
                File appPluginDir = null;
                String tisAppName = null;
                for (URL cp : libraryURLs) {
                    // 从对应的资源中将对应的plugin的目录解析出来，放到data目录下去

                    JarFile jar = new JarFile(new File(cp.toURI()));
                    Manifest manifest = jar.getManifest();

                    tisAppName = getTisAppName(cp, manifest);
                    //  String entryPrefix = tisAppName + "/";

                    File pluginLibDir = Config.getPluginLibDir("flink/" + tisAppName, true);
//                    if (!pluginLibDir.exists()) {
//                        throw new IllegalStateException("pluginLibDir can not empty:" + pluginLibDir.getAbsolutePath());
//                    }
//                    FileUtils.forceMkdir(pluginLibDir);
//                    File webInf = pluginLibDir.getParentFile();
//
//                    File classDir = new File(webInf, "classes");
                    appPluginDir = new File(pluginLibDir, "../.."); // webInf.getParentFile();
//                    AtomicInteger appRelevantResCount = new AtomicInteger();
//                    jar.stream().forEach((entry) -> {
//                        if (entry.isDirectory() || !StringUtils.startsWith(entry.getName(), entryPrefix)) {
//                            return;
//                        }
//                        try (InputStream entryStream = jar.getInputStream(entry)) {
//                            try (OutputStream out = FileUtils.openOutputStream(
//                                    new File(classDir, StringUtils.substringAfter(entry.getName(), entryPrefix)), false)) {
//                                IOUtils.copy(entryStream, out);
//                                appRelevantResCount.incrementAndGet();
//                            }
//                        } catch (IOException e) {
//                            throw new RuntimeException(entry.getName(), e);
//                        }
//                    });
//                    if (appRelevantResCount.get() < 1) {
//                        throw new IllegalStateException("appName:" + tisAppName + " relevant classpath res can not find in " + cp);
//                    }
                    // pluginRoot
                    // 写入MANIFEST文件
                    // pluginRoot
//                    try (OutputStream manOutput = FileUtils.openOutputStream(new File(appPluginDir, PluginWrapper.MANIFEST_FILENAME), false)) {
//                        manifest.write(manOutput);
//                    }

                    break;
                }

                if (StringUtils.isBlank(tisAppName)) {
                    throw new IllegalStateException("param tisAppName can not be empty");
                }
                if (appPluginDir == null || !appPluginDir.exists()) {
                    throw new IllegalStateException("appPluginDir can not be empty,path:" + appPluginDir.getAbsolutePath());
                }
                final String shotName = TISSinkFactory.KEY_FLINK_STREAM_APP_NAME_PREFIX + tisAppName;

                pluginManager.dynamicLoad(shotName, appPluginDir, true, null);

                return FlinkUserCodeClassLoaders.create(
                        classLoaderResolveOrder,
                        libraryURLs,
                        TISFlinClassLoaderFactory.class.getClassLoader(),
                        alwaysParentFirstPatterns,
                        NOOP_EXCEPTION_HANDLER,
                        checkClassLoaderLeak);
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        };
    }

    private void makeDataDirUseable() {
        try {
            FileUtils.forceMkdir(Config.getDataDir(false));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    @Override
    public BlobLibraryCacheManager.ClassLoaderFactory buildServerLoaderFactory(
            FlinkUserCodeClassLoaders.ResolveOrder classLoaderResolveOrder
            , String[] alwaysParentFirstPatterns, @Nullable Consumer<Throwable> exceptionHander, boolean checkClassLoaderLeak) {

        if (Boolean.getBoolean(SKIP_CLASSLOADER_FACTORY_CREATION)) {

            return (urls) -> FlinkUserCodeClassLoaders.create(classLoaderResolveOrder,
                    urls,
                    TISFlinClassLoaderFactory.class.getClassLoader(),
                    alwaysParentFirstPatterns,
                    NOOP_EXCEPTION_HANDLER,
                    checkClassLoaderLeak);
        }
        this.makeDataDirUseable();
        logger.info("buildServerLoader classLoaderResolveOrder:{}", classLoaderResolveOrder);
        return new BlobLibraryCacheManager.DefaultClassLoaderFactory(classLoaderResolveOrder
                , alwaysParentFirstPatterns, exceptionHander, checkClassLoaderLeak) {
            @Override
            public URLClassLoader createClassLoader(URL[] libraryURLs) {
                try {
                    String appName = getTisAppName(libraryURLs);
                    logger.info("start createClassLoader of app:" + appName);
                    TIS.clean();
                    synchronizeIncrPluginsFromRemoteRepository(appName);
                    PluginManager pluginManager = TIS.get().getPluginManager();
                    return new TISChildFirstClassLoader(pluginManager.uberClassLoader, libraryURLs, this.getParentClassLoader()
                            , this.alwaysParentFirstPatterns, this.classLoadingExceptionHandler);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

    public static void synchronizeIncrPluginsFromRemoteRepository(String appName) {

        if (CenterResource.notFetchFromCenterRepository()) {
            return;
        }

        TIS.permitInitialize = false;
        try {
            if (StringUtils.isBlank(appName)) {
                throw new IllegalArgumentException("param appName can not be null");
            }

            List<IRepositoryResource> keyedPluginStores = Lists.newArrayList();

            keyedPluginStores.add(new KeyedPluginStore(new KeyedPluginStore.AppKey(null, false, appName, MQListenerFactory.class)));
            keyedPluginStores.add(new KeyedPluginStore(new KeyedPluginStore.AppKey(null, false, appName, IncrStreamFactory.class)));
            keyedPluginStores.add(new KeyedPluginStore(new KeyedPluginStore.AppKey(null, false, appName, TISSinkFactory.class)));
//            keyedPluginStores.add(DataxWriter.getPluginStore(null, dataxName));
            ComponentMeta dataxComponentMeta = new ComponentMeta(keyedPluginStores);
            dataxComponentMeta.synchronizePluginsFromRemoteRepository();

        } finally {
            TIS.permitInitialize = true;
        }
    }

    private static String getTisAppName(URL[] libraryURLs) throws IOException {
        if (libraryURLs.length != 1) {
            throw new IllegalStateException("length of libraryURLs must be 1 , but now is:" + libraryURLs.length);
        }
        String appName = null;
        for (URL lib : libraryURLs) {
            try (JarInputStream jarReader = new JarInputStream(lib.openStream())) {
                Manifest manifest = jarReader.getManifest();
                appName = getTisAppName(lib, manifest);

                Attributes sysProps = manifest.getAttributes(Config.KEY_JAVA_RUNTIME_PROP_ENV_PROPS);
                Config.setConfig(null);
                // @see TISFlinkCDCStreamFactory 在这个类中进行配置信息的加载
                System.setProperty(Config.KEY_JAVA_RUNTIME_PROP_ENV_PROPS, String.valueOf(true));
                StringBuffer sysPropsDesc = new StringBuffer();
                for (Map.Entry<Object, Object> pluginDesc : sysProps.entrySet()) {
                    Attributes.Name name = (Attributes.Name) pluginDesc.getKey();
                    String val = (String) pluginDesc.getValue();
                    System.setProperty(TISFlinkCDCStart.convertCfgPropertyKey(name.toString(), false), val);
                    sysPropsDesc.append("\n").append(name.toString()).append("->").append(val);
                }
                logger.info("sysProps details:" + sysPropsDesc.toString());
                // shall not have any exception here.
                Config.getInstance();

            }
            if (StringUtils.isEmpty(appName)) {
                throw new IllegalStateException("param appName can not be null,in lib:" + lib.toString());
            }
        }

        return appName;
    }

    private static String getTisAppName(URL lib, Manifest manifest) {
        Attributes tisAppName = manifest.getAttributes(TISFlinkCDCStart.TIS_APP_NAME);
        String appName = null;
        //  Attributes pluginInventory = manifest.getAttributes("plugin_inventory");
        if (tisAppName == null) {
            throw new IllegalStateException("tisAppName can not be empty in lib:" + lib);
        }

        aa:
        for (Map.Entry<Object, Object> pluginDesc : tisAppName.entrySet()) {
            Attributes.Name name = (Attributes.Name) pluginDesc.getKey();
            String val = (String) pluginDesc.getValue();
            appName = name.toString();
            break aa;
            //  pluginManager.dynamicLoadPlugin(String.valueOf(pluginDesc.getKey()));
        }
        return appName;
    }
}
