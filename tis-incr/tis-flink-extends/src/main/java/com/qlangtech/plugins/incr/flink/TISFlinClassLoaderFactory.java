/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.plugins.incr.flink;

import com.google.common.collect.Lists;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.async.message.client.consumer.impl.MQListenerFactory;
import com.qlangtech.tis.datax.impl.DataxReader;
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
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.jar.Attributes;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;

import static org.apache.flink.util.FlinkUserCodeClassLoader.NOOP_EXCEPTION_HANDLER;


public class TISFlinClassLoaderFactory implements ClassLoaderFactoryBuilder {

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


    @Override
    public BlobLibraryCacheManager.ClassLoaderFactory buildClientLoaderFactory(
            FlinkUserCodeClassLoaders.ResolveOrder classLoaderResolveOrder
            , String[] alwaysParentFirstPatterns
            , @Nullable Consumer<Throwable> exceptionHander, boolean checkClassLoaderLeak) {
        try {
            FileUtils.forceMkdir(Config.getDataDir(false));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        ClassLoader parentClassLoader = TIS.get().getPluginManager().uberClassLoader;

        return (libraryURLs) -> {
            return FlinkUserCodeClassLoaders.create(
                    classLoaderResolveOrder,
                    libraryURLs,
                    parentClassLoader,
                    alwaysParentFirstPatterns,
                    NOOP_EXCEPTION_HANDLER,
                    checkClassLoaderLeak);
        };
    }


    @Override
    public BlobLibraryCacheManager.ClassLoaderFactory buildServerLoaderFactory(
            FlinkUserCodeClassLoaders.ResolveOrder classLoaderResolveOrder
            , String[] alwaysParentFirstPatterns, @Nullable Consumer<Throwable> exceptionHander, boolean checkClassLoaderLeak) {

        return new BlobLibraryCacheManager.DefaultClassLoaderFactory(classLoaderResolveOrder
                , alwaysParentFirstPatterns, exceptionHander, checkClassLoaderLeak) {
            @Override
            public URLClassLoader createClassLoader(URL[] libraryURLs) {
                try {

                    String appName = getTisAppName(libraryURLs);
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

            // KeyedPluginStore<DataxProcessor> processStore = IAppSource.getPluginStore(null, dataxName);
            List<IRepositoryResource> keyedPluginStores = Lists.newArrayList();// Lists.newArrayList(DataxReader.getPluginStore(dataxName), DataxWriter.getPluginStore(dataxName));
            //keyedPluginStores.add(TIS.getPluginStore(ParamsConfig.class));
            // keyedPluginStores.add(TIS.getPluginStore(FileSystemFactory.class));
            // keyedPluginStores.add(processStore);
            keyedPluginStores.add(new KeyedPluginStore(new DataxReader.AppKey(null, appName, MQListenerFactory.class)));
            keyedPluginStores.add(new KeyedPluginStore(new DataxReader.AppKey(null, appName, IncrStreamFactory.class)));
            keyedPluginStores.add(new KeyedPluginStore(new DataxReader.AppKey(null, appName, TISSinkFactory.class)));
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
                Attributes tisAppName = manifest.getAttributes(TISFlinkCDCStart.TIS_APP_NAME);
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
}
