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

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.extension.PluginManager;
import com.qlangtech.tis.extension.UberClassLoader;
import com.qlangtech.tis.manage.common.Config;
import com.qlangtech.tis.plugin.PluginAndCfgsSnapshot;
import com.qlangtech.tis.plugin.incr.TISSinkFactory;
import com.qlangtech.tis.util.XStream2;
import com.qlangtech.tis.web.start.TisAppLaunchPort;
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
import java.util.Map;
import java.util.function.Consumer;
import java.util.jar.Attributes;
import java.util.jar.JarFile;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;
import java.util.stream.Collectors;

import static org.apache.flink.util.FlinkUserCodeClassLoader.NOOP_EXCEPTION_HANDLER;


public class TISFlinkClassLoaderFactory implements ClassLoaderFactoryBuilder {

    public static final String SKIP_CLASSLOADER_FACTORY_CREATION = "skip_classloader_factory_creation";

    private static final Logger logger = LoggerFactory.getLogger(TISFlinkClassLoaderFactory.class);

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

                    File pluginLibDir = Config.getPluginLibDir(TISSinkFactory.KEY_PLUGIN_TPI_CHILD_PATH + tisAppName, true);
                    appPluginDir = new File(pluginLibDir, "../..");
                    appPluginDir = appPluginDir.toPath().normalize().toFile();
                    break;
                }

                if (StringUtils.isBlank(tisAppName)) {
                    throw new IllegalStateException("param tisAppName can not be empty");
                }
                if (appPluginDir == null || !appPluginDir.exists()) {
                    throw new IllegalStateException("appPluginDir can not be empty,path:" + appPluginDir.getAbsolutePath());
                }
                final String shotName = TISSinkFactory.KEY_PLUGIN_TPI_CHILD_PATH + tisAppName;

                pluginManager.dynamicLoad(shotName, appPluginDir, true, null);

                return FlinkUserCodeClassLoaders.create(
                        classLoaderResolveOrder,
                        libraryURLs,
                        pluginManager.uberClassLoader,
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
                    TIS.get().getPluginManager().uberClassLoader,
                    //TISFlinkClassLoaderFactory.class.getClassLoader(),
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
                    //  boolean tisInitialized = TIS.initialized;
                    PluginAndCfgsSnapshot cfgSnapshot = getTisAppName(libraryURLs);
                    logger.info("start createClassLoader of app:" + cfgSnapshot.getAppName().getName());
                    // TIS.clean();
                    // 这里只需要类不需要配置文件了
                    XStream2.PluginMeta flinkPluginMeta
                            = new XStream2.PluginMeta(TISSinkFactory.KEY_PLUGIN_TPI_CHILD_PATH + cfgSnapshot.getAppName().getName()
                            , Config.getMetaProps().getVersion());
                    // 服务端不需要配置文件，只需要能够加载到类就行了
                    PluginAndCfgsSnapshot localSnaphsot = PluginAndCfgsSnapshot.getLocalPluginAndCfgsSnapshot(cfgSnapshot.getAppName(), flinkPluginMeta);
                    cfgSnapshot.synchronizTpisAndConfs(localSnaphsot);

//                    for (XStream2.PluginMeta update : shallUpdate) {
//                        update.copyFromRemote(Collections.emptyList(), true, true);
//                    }
//
//                    PluginManager pluginManager = TIS.get().getPluginManager();
//                    Set<XStream2.PluginMeta> loaded = Sets.newHashSet();
//                    for (XStream2.PluginMeta update : shallUpdate) {
//                        dynamicLoad(pluginManager, update, batch, shallUpdate, loaded);
//                        // pluginManager.dynamicLoad(update.getPluginPackageFile(), true, batch);
//                    }
//
//                    //if (tisInitialized) {
//
//                    pluginManager.start(batch);
//                    } else {
//                        TIS.clean();
//                    }

                    return new TISChildFirstClassLoader(new UberClassLoader(TIS.get().getPluginManager(), cfgSnapshot.getPluginNames()), libraryURLs, this.getParentClassLoader()
                            , this.alwaysParentFirstPatterns, this.classLoadingExceptionHandler);
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }


//    public static void synchronizeIncrPluginsFromRemoteRepository(String appName) {
//
//        if (CenterResource.notFetchFromCenterRepository()) {
//            return;
//        }
//
//        TIS.permitInitialize = false;
//        try {
//            if (StringUtils.isBlank(appName)) {
//                throw new IllegalArgumentException("param appName can not be null");
//            }
//
//            List<IRepositoryResource> keyedPluginStores = Lists.newArrayList();
//
//            keyedPluginStores.add(new KeyedPluginStore(new KeyedPluginStore.AppKey(null, false, appName, MQListenerFactory.class)));
//            keyedPluginStores.add(new KeyedPluginStore(new KeyedPluginStore.AppKey(null, false, appName, IncrStreamFactory.class)));
//            keyedPluginStores.add(new KeyedPluginStore(new KeyedPluginStore.AppKey(null, false, appName, TISSinkFactory.class)));
//            ComponentMeta dataxComponentMeta = new ComponentMeta(keyedPluginStores);
//            dataxComponentMeta.synchronizePluginsFromRemoteRepository();
//
//
//        } finally {
//            TIS.permitInitialize = true;
//        }
//    }

    private static PluginAndCfgsSnapshot getTisAppName(URL[] libraryURLs) throws IOException {
        if (libraryURLs.length != 1) {
            throw new IllegalStateException("length of libraryURLs must be 1 , but now is:" + libraryURLs.length);
        }
        PluginAndCfgsSnapshot pluginAndCfgsSnapshot = null;
        String appName = null;
        for (URL lib : libraryURLs) {
            try (JarInputStream jarReader = new JarInputStream(lib.openStream())) {
                Manifest manifest = jarReader.getManifest();
                appName = getTisAppName(lib, manifest);

                // KeyedPluginStore.PluginMetas.KEY_GLOBAL_PLUGIN_STORE;
                //Attributes pluginMetas = manifest.getAttributes(Config.KEY_PLUGIN_METAS);
                // processPluginMetas(pluginMetas);

                pluginAndCfgsSnapshot = PluginAndCfgsSnapshot.setLocalPluginAndCfgsSnapshot(PluginAndCfgsSnapshot.deserializePluginAndCfgsSnapshot(new TargetResName(appName), manifest));

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
                TisAppLaunchPort.getPort();
                Config.getInstance();

            }
            if (pluginAndCfgsSnapshot == null) {
                throw new IllegalStateException("param appName can not be null,in lib:" + lib.toString());
            }
        }

        return pluginAndCfgsSnapshot;
    }

//    private static void processPluginMetas(Attributes pluginMetas) {
//        pluginMetas.getValue(KeyedPluginStore.PluginMetas.KEY_GLOBAL_PLUGIN_STORE);
//        pluginMetas.getValue(KeyedPluginStore.PluginMetas.KEY_PLUGIN_META);
//        pluginMetas.getValue(KeyedPluginStore.PluginMetas.KEY_APP_LAST_MODIFY_TIMESTAMP);
//    }

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
