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

package com.qlangtech.tis.plugin.ontology.impl.action;

import com.alibaba.fastjson.JSONObject;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.util.GroovyShellUtil;
import com.qlangtech.tis.extension.util.PluginExtraProps;
import com.qlangtech.tis.plugin.ontology.Ontology;
import com.qlangtech.tis.plugin.ontology.OntologyDomain;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.util.DefaultDescriptorsJSON;
import com.qlangtech.tis.util.DescriptorsMeta;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

/**
 * 把「参数池」动态编译成一个扁平的运行时表单 Descriptor，供 Action Type 详情页右侧的
 * 「表单预览」实时渲染，并在用户保存后落盘成 groovy + json 两个文件，作为执行期的稳定渲染源。
 *
 * <p>为什么必须走 groovy 代码生成：{@link com.qlangtech.tis.extension.impl.PropertyType}
 * 构造器强制要求真实 {@link java.lang.reflect.Field}（且字段必须是 public 非 static），
 * 纯 Java 运行时拼不出可序列化的 PropertyType；groovy 是现成的生成器。
 *
 * <p>三个使用场景（同一套「参数池 → 字段元数据 → groovy 源码 + 注入 extra-props」推导）：
 * <ol>
 *   <li><b>预览</b>{@link #build}：内存编译不落盘，结果按内容哈希缓存，同结构重复请求 O(1)；</li>
 *   <li><b>保存</b>{@link #writeToDisk}：把 groovy 源码与描述 JSON 写到 {@code form/}；</li>
 *   <li><b>执行</b>{@link #loadFromDisk}：读两个文件编译 + 注入，不再解析 ActionParameters。</li>
 * </ol>
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/16
 */
public final class ActionFormDescriptorBuilder {

    /**
     * form/ 子目录名，与 params/ 平行落在 {@code <cfgRoot>/ontology/<domain>/action-type/} 下。
     */
    public static final String KEY_FORM_SUB_DIR = "form";

    /**
     * 生成的 groovy 类名固定，避免频繁触发时元空间累积（同名类 {@code setClassCacheEntry} 覆盖）。
     */
    private static final String CLASS_NAME = "ActionFormPreview";
    private static final String DESC_CLASS_NAME = "Desc";
    private static final String PKG = "com.qlangtech.tis.plugin.ontology.impl.action";

    /**
     * 预览结果的 content-hash 缓存：只有参数「结构」变化时才重新编译。
     */
    private static final Cache<String, DescriptorsMeta> PREVIEW_CACHE = CacheBuilder.newBuilder()
            .maximumSize(256)
            .expireAfterAccess(30, TimeUnit.MINUTES)
            .build();

    private ActionFormDescriptorBuilder() {
    }

    // ---------------------------------------------------------------------
    // 对外入口
    // ---------------------------------------------------------------------

    /**
     * 预览入口：把参数池推导成扁平表单 Descriptor 并序列化回 {@link DescriptorsMeta}。
     *
     * <p>结果按内容哈希缓存。注意缓存复用同一个 {@link DescriptorsMeta} 实例：它只被
     * fastjson 序列化回传、从不回写，复用是安全的。
     */
    public static DescriptorsMeta build(OntologyPluginMeta paramMeta, ActionParameters params) {
        String hash = contentHash(paramMeta,params);
        DescriptorsMeta cached = PREVIEW_CACHE.getIfPresent(hash);
        if (cached != null) {
            return cached;
        }
        BuildResult result = buildUncached(paramMeta, params);
        PREVIEW_CACHE.put(hash, result.meta);
        return result.meta;
    }

    /**
     * 执行入口：用当前 Action 已持久化的参数池现场生成运行时 Descriptor。
     *
     * <p>生成类不在 Sezpoz 扩展索引里，{@code TIS.get().getDescriptor(impl)} 永远查不到，
     * 因此这里把 descriptor 实例直接返回给调用方（{@code AttrValMap} 支持显式传入）。
     */
    public static Descriptor<?> buildDescriptor(OntologyPluginMeta paramMeta, ActionParameters params) {
        return buildUncached(paramMeta, params).descriptor;
    }

    /**
     * 保存入口：把 groovy 源码与描述 JSON 落盘到
     * {@code <cfgRoot>/ontology/<domain>/action-type/form/<actionName>.groovy|.json}。
     *
     * <p>执行期（{@link #loadFromDisk}）读这两个文件即可还原 Descriptor，不再解析参数池。
     */
    public static void writeToDisk(OntologyPluginMeta paramMeta, ActionParameters params, String domain, String actionName) throws Exception {
        BuildResult result = buildUncached(paramMeta, params);
        java.io.File formDir = Ontology.OntologyEnum.Action.storeKeyGetter
                .getAssistRootDir(domain, Optional.of(KEY_FORM_SUB_DIR));
        if (!formDir.exists() && !formDir.mkdirs()) {
            throw new IllegalStateException("can not create dir:" + formDir.getAbsolutePath());
        }
        Files.writeString(new java.io.File(formDir, actionName + ".groovy").toPath(),
                result.groovySource, StandardCharsets.UTF_8);
        Files.writeString(new java.io.File(formDir, actionName + ".json").toPath(),
                result.extraPropsJson, StandardCharsets.UTF_8);
    }

    /**
     * 执行入口：读回落盘的 groovy + json，编译并注入 extra-props，返回 Descriptor 序列化结果。
     */
    public static DescriptorsMeta loadFromDisk(String domain, String actionName) throws Exception {
        java.io.File formDir = Ontology.OntologyEnum.Action.storeKeyGetter
                .getAssistRootDir(domain, Optional.of(KEY_FORM_SUB_DIR));
        Path groovyPath = new java.io.File(formDir, actionName + ".groovy").toPath();
        Path jsonPath = new java.io.File(formDir, actionName + ".json").toPath();
        if (!Files.exists(groovyPath)) {
            throw new IllegalStateException("groovy source not found:" + groovyPath);
        }
        String groovySource = Files.readString(groovyPath, StandardCharsets.UTF_8);
        String extraPropsJson = Files.exists(jsonPath)
                ? Files.readString(jsonPath, StandardCharsets.UTF_8) : "{}";

        Descriptor<?> descriptor = compileDescriptor(groovySource);
        injectExtraProps(descriptor, extraPropsJson);
        return serialize(descriptor);
    }

    // ---------------------------------------------------------------------
    // 核心推导
    // ---------------------------------------------------------------------

    /**
     * 一次「参数池 → groovy 源码 + 描述 JSON + Descriptor」的完整推导。
     */
    private static BuildResult buildUncached(OntologyPluginMeta paramMeta, ActionParameters params) {
        List<ActionParameterConfig> parameters = nonNullParameters(params);
        String groovySource = generateGroovySource(paramMeta, parameters);
        String extraPropsJson = generateExtraPropsJson(parameters);

        Descriptor<?> descriptor = compileDescriptor(groovySource);
        injectExtraProps(descriptor, extraPropsJson);
        DescriptorsMeta meta = serialize(descriptor);
        return new BuildResult(groovySource, extraPropsJson, meta, descriptor);
    }

    /**
     * 过滤掉参数池里可能的 null 行（部分表单态下会出现），源码与描述 JSON 共用同一份有效参数。
     */
    private static List<ActionParameterConfig> nonNullParameters(ActionParameters params) {
        return params.getParameters().stream().filter(Objects::nonNull).toList();
    }

    /**
     * 生成 groovy 源码。字段名直接取 parameterId（已由 identity_strict 约束为合法标识符），
     * 显示名由注入的 label 承载。
     */
    private static String generateGroovySource(OntologyPluginMeta paramMeta, List<ActionParameterConfig> parameters) {

        StringBuilder sb = new StringBuilder(512);
        sb.append("package ").append(PKG).append("\n\n");
        sb.append("import com.qlangtech.tis.extension.Describable\n");
        sb.append("import com.qlangtech.tis.extension.Descriptor\n");
        sb.append("import com.qlangtech.tis.plugin.annotation.FormField\n");
        sb.append("import com.qlangtech.tis.plugin.annotation.FormFieldType\n");
        sb.append("import com.qlangtech.tis.plugin.annotation.Validator\n");
        sb.append("import java.util.List\n");
        sb.append("import java.util.Map\n\n");
        sb.append("class ").append(CLASS_NAME).append(" implements Describable<").append(CLASS_NAME).append("> {\n\n");


        for (ActionParameterConfig param : parameters) {
            final int ordinal = Objects.requireNonNull(param.order, "order can not be null");
            sb.append("    @FormField(ordinal = ").append(ordinal)
                    .append(", type = FormFieldType.").append(param.formFieldType().name())
                    .append(", validate = ").append(param.required() ? "[Validator.require]" : "[]")
                    .append(")\n");
            sb.append("    public ").append(param.javaType()).append(" ").append(param.requireFieldName()).append("\n\n");

        }

        // 生成类不在 Sezpoz 扩展索引里，TIS.getDescriptor(Class) 查不到；执行链路上若有
        // plugin.getDescriptor() 调用会拿到 null，这里返回自带的 Desc 兜底。
        sb.append("    @Override\n");
        sb.append("    Descriptor getDescriptor() {\n");
        sb.append("        return new Desc()\n");
        sb.append("    }\n\n");

        sb.append("    static class ").append(DESC_CLASS_NAME).append(" extends Descriptor<")
                .append(CLASS_NAME).append("> {\n\n");
        sb.append("        String getDisplayName() {\n");
        sb.append("            return 'Action_Form_Preview'\n");
        sb.append("        }\n\n");
        // descriptor 级 extractProps（与 injectExtraProps 注入的「按字段」extra-props 是两套机制），
        // 序列化进 descriptor.extractProps，供前端读取。
        sb.append("        @Override\n");
        sb.append("        public Map<String, Object> getExtractProps(boolean forAIPromote) {\n");
        sb.append("            Map<String, Object> props = super.getExtractProps(forAIPromote);\n");
        // 注意别写成 OntologyDomain.ONTOLOGY_DOMAIN：那是 HeteroEnum 实例不是字符串，
        // 拼进 groovy 会得到 "…OntologyDomain$1@…" —— 双引号里的 $1 还会被当成 GString 插值，直接编译失败。
        // 值用单引号包裹，domain/actionName 即使含 $ 也不会被插值。
        sb.append("            props.put(\"" + OntologyDomain.NAME_ONTOLOGY_DOMAIN + "\", '" + paramMeta.getDomain() + "');\n");
        sb.append("            props.put(\"" + OntologyDomain.FIELD_ONTOLOGY_ACTION_NAME + "\", '" + paramMeta.getPluginIdVal() + "');\n");
        sb.append("            return props;\n");
        sb.append("        }\n");
        sb.append("    }\n");
        sb.append("}\n");
        return sb.toString();
    }

    /**
     * 描述 JSON：字段名 → 该字段的 extra-props（label/help/dftVal/enum…）。
     * 每个字段写哪些 key 由 {@link ActionParameterConfig#createExtraPropsJson} 决定，
     * 这里只做逐字段聚合；落盘后由 {@link #injectExtraProps} 回灌。
     */
    private static String generateExtraPropsJson(List<ActionParameterConfig> parameters) {
        JSONObject root = new JSONObject();
        for (ActionParameterConfig param : parameters) {
            JSONObject fieldJson = param.createExtraPropsJson();
            root.put(param.requireFieldName(), fieldJson);
        }
        return root.toJSONString();
    }

    /**
     * 编译 groovy 源码并反射取出嵌套的 {@code Desc} descriptor 实例。
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static Descriptor<?> compileDescriptor(String groovySource) {
        GroovyShellUtil.loadMyClass(CLASS_NAME, groovySource);
        Class<?> groovyClass = GroovyShellUtil.loadClass(PKG, CLASS_NAME);
        for (Class<?> nested : groovyClass.getDeclaredClasses()) {
            if (Descriptor.class.isAssignableFrom(nested)) {
                try {
                    return (Descriptor<?>) nested.getDeclaredConstructor().newInstance();
                } catch (ReflectiveOperationException e) {
                    throw new IllegalStateException("can not instantiate nested descriptor:" + nested.getName(), e);
                }
            }
        }
        throw new IllegalStateException("nested descriptor not found in:" + groovyClass.getName());
    }

    /**
     * 把描述 JSON 回灌进 {@code fieldExtraDescs}，从而合并进最终序列化结果。
     */
    private static void injectExtraProps(Descriptor<?> descriptor, String extraPropsJson) {
        JSONObject root = JSONObject.parseObject(extraPropsJson);
        for (Map.Entry<String, Object> entry : root.entrySet()) {
            // 与 PluginExtraProps#parseExtraProps 的内层循环同构：每个字段的原始 JSONObject 原样包成 Props，
            // 所有 key（label/dftVal/help/enum/…）原样保留 —— 后续在 json 里新增 key 无需改这里。
            // 刻意不走 addFieldDescriptor：它只挑 dftVal/label/disabled/readonly/enum 这几个 key，会丢弃其余属性。
            descriptor.fieldExtraDescs.put(entry.getKey(),
                    new PluginExtraProps.Props((JSONObject) entry.getValue()));
        }
    }

    /**
     * 序列化 descriptor 为前端消费的 {@link DescriptorsMeta}。
     */
    @SuppressWarnings({"rawtypes", "unchecked"})
    private static DescriptorsMeta serialize(Descriptor<?> descriptor) {
        return new DefaultDescriptorsJSON(descriptor).getDescriptorsJSON();
    }

    // ---------------------------------------------------------------------
    // 工具
    // ---------------------------------------------------------------------

    /**
     * 内容哈希：groovy 源码 + 描述 JSON 的 SHA-256，是预览缓存的 key。
     */
    private static String contentHash(OntologyPluginMeta paramMeta, ActionParameters params) {
        // 哈希只需 groovy 源码 + 描述 JSON，不必真编译；但复用推导能保证 key 与产物严格一致。
        List<ActionParameterConfig> parameters = nonNullParameters(params);
        String src = generateGroovySource(paramMeta, parameters);
        String json = generateExtraPropsJson(parameters);
        return sha256(src + '\n' + json);
    }

    private static String sha256(String input) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(input.getBytes(StandardCharsets.UTF_8));
            StringBuilder hex = new StringBuilder(hash.length * 2);
            for (byte b : hash) {
                hex.append(Character.forDigit((b >> 4) & 0xf, 16)).append(Character.forDigit(b & 0xf, 16));
            }
            return hex.toString();
        } catch (Exception e) {
            throw new IllegalStateException(e);
        }
    }

    /**
     * 一次推导的中间产物：groovy 源码、描述 JSON、序列化结果、运行时 descriptor。
     */
    private record BuildResult(String groovySource, String extraPropsJson, DescriptorsMeta meta,
                               Descriptor<?> descriptor) {
    }
}
