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

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.extension.PluginManager;
import org.apache.flink.runtime.execution.librarycache.BlobLibraryCacheManager;
import org.apache.flink.runtime.execution.librarycache.ClassLoaderFactoryBuilder;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Map;
import java.util.function.Consumer;
import java.util.jar.Attributes;
import java.util.jar.JarInputStream;
import java.util.jar.Manifest;


public class TISFlinClassLoaderFactory implements ClassLoaderFactoryBuilder {

    @Override
    public BlobLibraryCacheManager.ClassLoaderFactory buildServerLoaderFactory(
            FlinkUserCodeClassLoaders.ResolveOrder classLoaderResolveOrder
            , String[] alwaysParentFirstPatterns, @Nullable Consumer<Throwable> exceptionHander, boolean checkClassLoaderLeak) {

        return new BlobLibraryCacheManager.DefaultClassLoaderFactory(classLoaderResolveOrder
                , alwaysParentFirstPatterns, exceptionHander, checkClassLoaderLeak) {

            @Override
            public URLClassLoader createClassLoader(URL[] libraryURLs) {

                try {
                    PluginManager pluginManager = TIS.get().getPluginManager();
                    if (libraryURLs.length != 1) {
                        throw new IllegalStateException("length of libraryURLs must be 1 , but now is:" + libraryURLs.length);
                    }
                    for (URL lib : libraryURLs) {
                        try (JarInputStream jarReader = new JarInputStream(lib.openStream())) {
                            Manifest manifest = jarReader.getManifest();
                            Attributes pluginInventory = manifest.getAttributes("plugin_inventory");
                            if (pluginInventory == null) {
                                throw new IllegalStateException("plugin inventory can not be empty in lib:" + lib);
                            }
                            for (Map.Entry<Object, Object> pluginDesc : pluginInventory.entrySet()) {
                                pluginManager.dynamicLoadPlugin(String.valueOf(pluginDesc.getKey()));
                            }
                        }
                    }
                    return new TISChildFirstClassLoader(pluginManager.uberClassLoader, libraryURLs, this.getParentClassLoader()
                            , this.alwaysParentFirstPatterns, this.classLoadingExceptionHandler);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }
}
