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

package com.qlangtech.tis.compiler.streamcode;

import com.alibaba.citrus.turbine.Context;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.compiler.incr.ICompileAndPackage;
import com.qlangtech.tis.compiler.java.FileObjectsContext;
import com.qlangtech.tis.compiler.java.MyJavaFileObject;
import com.qlangtech.tis.compiler.java.NestClassFileObject;
import com.qlangtech.tis.compiler.java.ResourcesFile;
import com.qlangtech.tis.compiler.java.SourceGetterStrategy;
import com.qlangtech.tis.compiler.java.ZipPath;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.datax.TimeFormat;
import com.qlangtech.tis.datax.job.SSERunnable;
import com.qlangtech.tis.extension.PluginManager;
import com.qlangtech.tis.extension.PluginStrategy;
import com.qlangtech.tis.extension.PluginWrapper;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.UberClassLoader;
import com.qlangtech.tis.extension.impl.PluginManifest;
import com.qlangtech.tis.manage.common.Config;
import com.qlangtech.tis.manage.common.incr.StreamContextConstant;
import com.qlangtech.tis.maven.plugins.tpi.PluginClassifier;
import com.qlangtech.tis.plugin.incr.TISSinkFactory;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.sql.parser.IDBNodeMeta;
import net.java.sezpoz.impl.Indexer;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.tools.ScalaCompilerSupport;
import scala.tools.scala_maven_executions.LogProcessorUtils;

import javax.tools.JavaFileObject;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectOutputStream;
import java.nio.file.Path;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;
import java.util.stream.Collectors;
import java.util.zip.ZipEntry;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-10-20 16:35
 **/
public class CompileAndPackage implements ICompileAndPackage {
    private static final Logger logger = LoggerFactory.getLogger(CompileAndPackage.class);
    private final List<PluginWrapper.Dependency> extraPluginDependencies;
    private final Set<PluginManifest> classInExtraPlugin;

    public CompileAndPackage(List<PluginWrapper.Dependency> extraPluginDependencies) {
        this(extraPluginDependencies, Collections.emptySet());
    }

    public CompileAndPackage(Set<Object> classInExtraPlugin) {
        this(Collections.emptyList(), classInExtraPlugin);
    }

    public CompileAndPackage() {
        this(Collections.emptyList());
    }

    private CompileAndPackage(List<PluginWrapper.Dependency> extraPluginDependencies, Set<Object> classInExtraPlugin) {
        if (extraPluginDependencies == null) {
            throw new IllegalArgumentException("param extraDependencyClasspaths can not be null");
        }
        this.extraPluginDependencies = extraPluginDependencies;
        final UberClassLoader clazzLoader = TIS.get().getPluginManager().uberClassLoader;
        this.classInExtraPlugin = classInExtraPlugin.stream().map((clazz) -> {
            Class<?> c = null;
            if (clazz instanceof Class) {
                c = ((Class<?>) clazz);
            } else if (clazz instanceof String) {
                try {
                    c = (clazzLoader.findClass((String) clazz));
                } catch (ClassNotFoundException e) {
                    throw new RuntimeException("clazz:" + clazz, e);
                }
            }
            if (c != null) {
                return PluginManifest.create(c);
            }
            throw new IllegalStateException("type error:" + clazz.getClass());
        }).collect(Collectors.toSet());
    }


    /**
     * @param context
     * @param msgHandler
     * @param appName
     * @param dbNameMap
     * @param sourceRoot
     * @param xmlConfigs 取得spring配置文件相关resourece
     * @throws Exception
     */
    @Override
    public File process(Context context, IControlMsgHandler msgHandler
            , String appName, Map<IDBNodeMeta, List<String>> dbNameMap, File sourceRoot, FileObjectsContext xmlConfigs) throws Exception {
        if (xmlConfigs == null) {
            throw new IllegalArgumentException("param xmlConfigs can not be null");
        }
        if (StringUtils.isEmpty(appName)) {
            throw new IllegalArgumentException("param appName can not be null");
        }
        /**
         * *********************************************************************************
         * 编译增量脚本
         * ***********************************************************************************
         */
        if (this.streamScriptCompile(sourceRoot, dbNameMap.keySet())) {
            msgHandler.addErrorMessage(context, "增量脚本编译失败");
            msgHandler.addFieldError(context, "incr_script_compile_error", "error");
            return null;
        }
        /**
         * *********************************************************************************
         * 对scala代码进行 打包
         * ***********************************************************************************
         */
        SourceGetterStrategy getterStrategy
                = new SourceGetterStrategy(false, "/src/main/scala", ".scala") {
            @Override
            public JavaFileObject.Kind getSourceKind() {
                // 没有scala的类型，暂且用other替换一下
                return JavaFileObject.Kind.OTHER;
            }

            @Override
            public MyJavaFileObject processMyJavaFileObject(MyJavaFileObject fileObj) {
                try {
                    try (InputStream input = FileUtils.openInputStream(fileObj.getSourceFile())) {
                        IOUtils.copy(input, fileObj.openOutputStream());
                    }
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
                return fileObj;
            }
        };

        FileObjectsContext fileObjects = FileObjectsContext.getFileObjects(sourceRoot, getterStrategy);

        final FileObjectsContext compiledCodeContext = new FileObjectsContext();
        File streamScriptClassesDir = new File(sourceRoot, "classes");
        appendClassFile(streamScriptClassesDir, compiledCodeContext, null);

        Manifest man = new Manifest();

        File pluginLibDir = Config.getPluginLibDir(TISSinkFactory.KEY_PLUGIN_TPI_CHILD_PATH + appName, false);
        FileUtils.forceMkdir(pluginLibDir);
        File webInf = pluginLibDir.getParentFile();
        File pluginDir = webInf.getParentFile();


        //====================================================================
        TargetResName collection = new TargetResName(appName);
        // 插件元数据
        Manifest tpiMeta = createPluginMetaInfo(collection);
        //====================================================================

        FileObjectsContext tisExtension = new FileObjectsContext();

        // 保证组件服务可以成功加载
        ByteArrayOutputStream bytes = null;
        try (ObjectOutputStream output = new ObjectOutputStream(bytes = new ByteArrayOutputStream())) {
            output.writeObject(SerAnnotatedElementUtils.create(collection));
            output.writeObject(null);
            output.flush();
            tisExtension.resources.add(
                    new ResourcesFile(new ZipPath(Indexer.METAINF_ANNOTATIONS, TISExtension.class.getName(), JavaFileObject.Kind.OTHER)
                            , bytes.toByteArray()));
        }

        File pkgJar = new File(pluginLibDir, StreamContextConstant.getIncrStreamJarName(appName));
        FileUtils.deleteQuietly(pkgJar);
        // 将stream code打包
        FileObjectsContext.packageJar(
                pkgJar
                , man
                , fileObjects, compiledCodeContext, xmlConfigs, tisExtension);

        // 继续打一个tpi包
        File tpi = new File(pluginDir.getParentFile(), pluginDir.getName() + PluginManager.PACAKGE_TPI_EXTENSION);
        FileUtils.deleteQuietly(tpi);
        File f = null;
        Path pluginRootPath = pluginDir.toPath();
        try (JarOutputStream jaroutput = new JarOutputStream(
                FileUtils.openOutputStream(tpi, false), tpiMeta)) {
            Iterator<File> fit = FileUtils.iterateFiles(pluginDir, null, true);
            while (fit.hasNext()) {
                f = fit.next();
                try {
                    jaroutput.putNextEntry(new ZipEntry(pluginRootPath.relativize(f.toPath()).toString()));
                    if (!f.isDirectory()) {
                        try (InputStream content = FileUtils.openInputStream(f)) {
                            jaroutput.write(IOUtils.toByteArray(content));
                        }
                    }
                    jaroutput.closeEntry();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }

            }
        }
        logger.info("pkgJar:{},tpi:{}", pkgJar.getAbsolutePath(), tpi.getAbsolutePath());
        return tpi;

//        FileObjectsContext.packageJar(
//
//                , man
//                , fileObjects, compiledCodeContext, xmlConfigs, tisExtension);
    }


    private Manifest createPluginMetaInfo(TargetResName collection) {
        Manifest man = new Manifest();
        Attributes mattrs = man.getMainAttributes();

        mattrs.put(Attributes.Name.MANIFEST_VERSION, "1.0");
        mattrs.put(new Attributes.Name(PluginStrategy.KEY_MANIFEST_SHORTNAME)
                , TISSinkFactory.KEY_PLUGIN_TPI_CHILD_PATH + collection.getName());
        mattrs.put(new Attributes.Name(PluginStrategy.KEY_MANIFEST_PLUGIN_VERSION), Config.getMetaProps().getVersion());
        mattrs.put(new Attributes.Name(PluginStrategy.KEY_MANIFEST_PLUGIN_FIRST_CLASSLOADER), "true");
        mattrs.put(new Attributes.Name(PluginStrategy.KEY_LAST_MODIFY_TIME), String.valueOf(System.currentTimeMillis()));

        mattrs.put(new Attributes.Name(PluginManager.PACAKGE_CLASSIFIER), PluginClassifier.MATCH_ALL_CLASSIFIER.getClassifier());

        if (CollectionUtils.isNotEmpty(this.extraPluginDependencies)
                || CollectionUtils.isNotEmpty(this.classInExtraPlugin)) {
            List<String> dpts = Lists.newArrayList();
            this.extraPluginDependencies.forEach((dpt) -> {
                dpts.add(dpt.shortName + ":" + dpt.version);
            });
            this.classInExtraPlugin.forEach((dpt) -> {
                String pluginName = dpt.computeShortName(StringUtils.EMPTY);
                dpts.add(pluginName + ":" + dpt.getVersionOf());
            });
            mattrs.put(new Attributes.Name(PluginStrategy.KEY_MANIFEST_DEPENDENCIES)
                    , dpts.stream().collect(Collectors.joining(",")));
        }
        return man;
//        try (OutputStream output = FileUtils.openOutputStream(
//                new File(webInf.getParentFile(), JarFile.MANIFEST_NAME), false)) {
//            man.write(output);
//        } catch (Exception e) {
//            throw new IllegalStateException(e);
//        }

    }

    private boolean streamScriptCompile(File sourceRoot, Set<IDBNodeMeta> dependencyDBNodes) throws Exception {
        final SSERunnable sse = SSERunnable.getLocal();
        LogProcessorUtils.LoggerListener loggerListener = new LogProcessorUtils.LoggerListener() {
            @Override
            public void receiveLog(LogProcessorUtils.Level level, String line) {
                sse.error(null, TimeFormat.getCurrentTimeStamp(), line);
                System.err.println(line);
            }
        };
        HashSet<String> depClasspath = Sets.newHashSet(IDBNodeMeta.appendDBDependenciesClasspath(dependencyDBNodes));
        depClasspath.addAll(this.extraPluginDependencies.stream().map((plugin) -> {
            return Config.getPluginLibDir(plugin.shortName).getAbsolutePath() + "/*";
        }).collect(Collectors.toList()));

        depClasspath.addAll(this.classInExtraPlugin.stream().flatMap((clazzInPlugin) -> {
            return clazzInPlugin.getClasspath().stream();
            // return clazzInPlugin.getPluginLibDir().getAbsolutePath() + "/*";
        }).collect(Collectors.toList()));

        return ScalaCompilerSupport.streamScriptCompile(sourceRoot, depClasspath, loggerListener);
    }

    private void appendClassFile(File parent, FileObjectsContext fileObjects, final StringBuffer qualifiedClassName) throws IOException {
        String[] children = parent.list();
        File childFile = null;
        for (String child : children) {
            childFile = new File(parent, child);
            if (childFile.isDirectory()) {
                StringBuffer newQualifiedClassName = null;
                if (qualifiedClassName == null) {
                    newQualifiedClassName = new StringBuffer(child);
                } else {
                    newQualifiedClassName = (new StringBuffer(qualifiedClassName)).append(".").append(child);
                }
                appendClassFile(childFile, fileObjects, newQualifiedClassName);
            } else {
                final String className = StringUtils.substringBeforeLast(child, ".");
                //
                NestClassFileObject fileObj = NestClassFileObject.getNestClassFileObject(
                        ((new StringBuffer(qualifiedClassName)).append(".").append(className)).toString(), fileObjects.classMap);
                try (InputStream input = FileUtils.openInputStream(childFile)) {
                    IOUtils.copy(input, fileObj.openOutputStream());
                }
            }
        }
    }

}
