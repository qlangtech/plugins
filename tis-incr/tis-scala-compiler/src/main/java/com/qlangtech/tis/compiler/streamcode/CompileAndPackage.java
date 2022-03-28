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
import com.google.common.collect.Sets;
import com.qlangtech.tis.compiler.incr.ICompileAndPackage;
import com.qlangtech.tis.compiler.java.FileObjectsContext;
import com.qlangtech.tis.compiler.java.MyJavaFileObject;
import com.qlangtech.tis.compiler.java.NestClassFileObject;
import com.qlangtech.tis.compiler.java.SourceGetterStrategy;
import com.qlangtech.tis.extension.PluginStrategy;
import com.qlangtech.tis.extension.PluginWrapper;
import com.qlangtech.tis.manage.common.Config;
import com.qlangtech.tis.manage.common.incr.StreamContextConstant;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.sql.parser.IDBNodeMeta;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import scala.tools.ScalaCompilerSupport;
import scala.tools.scala_maven_executions.LogProcessorUtils;

import javax.tools.JavaFileObject;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.jar.Manifest;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-10-20 16:35
 **/
public class CompileAndPackage implements ICompileAndPackage {

    private final List<PluginWrapper.Dependency> extraPluginDependencies;

    public CompileAndPackage(List<PluginWrapper.Dependency> extraPluginDependencies) {
        if (extraPluginDependencies == null) {
            throw new IllegalArgumentException("param extraDependencyClasspaths can not be null");
        }
        this.extraPluginDependencies = extraPluginDependencies;
    }

    public CompileAndPackage() {
        this(Collections.emptyList());
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
    public void process(Context context, IControlMsgHandler msgHandler
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
            return;
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
        //
        FileObjectsContext fileObjects = FileObjectsContext.getFileObjects(sourceRoot, getterStrategy);
        final FileObjectsContext compiledCodeContext = new FileObjectsContext();
        File streamScriptClassesDir = new File(sourceRoot, "classes");
        appendClassFile(streamScriptClassesDir, compiledCodeContext, null);

        Manifest man = new Manifest();
        if (CollectionUtils.isNotEmpty(this.extraPluginDependencies)) {
            man.getMainAttributes().put(PluginStrategy.KEY_MANIFEST_DEPENDENCIES
                    , this.extraPluginDependencies.stream().map((dpt) -> dpt.shortName + ":" + dpt.version)
                            .collect(Collectors.joining(",")));
        }

        // 将stream code打包
        FileObjectsContext.packageJar(
                sourceRoot, StreamContextConstant.getIncrStreamJarName(appName), man
                , fileObjects, compiledCodeContext, xmlConfigs);
    }

    private boolean streamScriptCompile(File sourceRoot, Set<IDBNodeMeta> dependencyDBNodes) throws Exception {
        LogProcessorUtils.LoggerListener loggerListener = new LogProcessorUtils.LoggerListener() {

            @Override
            public void receiveLog(LogProcessorUtils.Level level, String line) {
                System.err.println(line);
            }
        };
        HashSet<String> depClasspath = Sets.newHashSet(IDBNodeMeta.appendDBDependenciesClasspath(dependencyDBNodes));
        depClasspath.addAll(this.extraPluginDependencies.stream().map((plugin) -> {
            return Config.getPluginLibDir(plugin.shortName).getAbsolutePath() + "/*";
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
