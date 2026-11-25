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

package com.qlangtech.tis.plugin.ontology.impl.action.type;

import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.IPropertyType;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.PropertyType;
import com.qlangtech.tis.ibatis.RowMap;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.transformer.UDFDesc;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.IDBReservedKeys;
import com.qlangtech.tis.plugin.ds.JDBCConnection;
import com.qlangtech.tis.plugin.ontology.Ontology;
import com.qlangtech.tis.plugin.ontology.OntologyObjectType;
import com.qlangtech.tis.plugin.ontology.OntologyProperty;
import com.qlangtech.tis.plugin.ontology.OntologyType;
import com.qlangtech.tis.plugin.ontology.OntologyValueType;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.plugin.ontology.impl.action.ActionParameterConfig;
import com.qlangtech.tis.plugin.ontology.impl.action.ActionParameterMapper;
import com.qlangtech.tis.plugin.ontology.impl.action.ActionParameters;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.AdapterRuleVisitor;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.CreateObjectRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.DeleteObjectRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.FromParameterValueSource;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.ModifyObjectRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.OntologyActionRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.PropertyMappingConfig;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.RuleGroup;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.RuleType;
import com.qlangtech.tis.plugin.ontology.impl.linker.LinkReference;
import com.qlangtech.tis.plugin.ontology.impl.objtype.DataSourceBinding;
import com.qlangtech.tis.plugin.ontology.impl.objtype.ObjectTypeBinding;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * Object Action Type - 对象操作
 * <p>
 * 对应 Ontology Object 操作，包括：
 * - CREATE_OBJECT: 创建新对象
 * - MODIFY_OBJECT: 修改现有对象
 * - DELETE_OBJECT: 删除对象
 * - CREATE_OR_MODIFY_OBJECT: 创建或修改对象
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/9/7
 */
public class ObjectActionType extends ActionType {
    public static final String KEY_OBJECT_TYPE_REFERENCE = "objectTypeReference";
    public static final String KEY_OBJECT_RULE = "objectRule";

    /**
     * 参数命名空间的分隔符。多对象类型 Action 里参数名形如 {@code user_id}。
     */
    public static final char NAMESPACE_SEPARATOR = '_';

    @FormField(ordinal = 0, type = FormFieldType.SELECTABLE, validate = {Validator.require})
    public String objectTypeReference;

    @Override
    public List<UDFDesc> getLiteria() {
        List<UDFDesc> desc = Lists.newArrayList(new UDFDesc(OntologyObjectType.KEY_OBJECT_TYPE, objectTypeReference));
        desc.addAll(objectRule.getLiteria());
        return desc;
    }

    /**
     * @see CreateObjectRule
     * @see com.qlangtech.tis.plugin.ontology.impl.action.rule.ModifyObjectRule
     * @see com.qlangtech.tis.plugin.ontology.impl.action.rule.DeleteObjectRule
     */
    @FormField(ordinal = 1, validate = {Validator.require})
    public OntologyActionRule objectRule;

    public OntologyObjectType loadObjectType() {
        OntologyPluginMeta meta = OntologyPluginMeta.createPluginMeta();
        return Ontology.loadObjectTypeDetail(meta.getDomain(), objectTypeReference);
    }

    /**
     * 显式指定 ontology domain 加载对象类型。执行期经 {@link #executeActionRule(Describable)}
     * 走线程局部取域；此处保留显式 domain 的重载供 {@code DefaultOntologyAction#resolveObjectIdentityParam}
     * 等已知 domain 的调用点使用，避免依赖线程局部。
     */
    public OntologyObjectType loadObjectType(String domain) {
        return Ontology.loadObjectTypeDetail(domain, objectTypeReference);
    }

    @Override
    public void executeActionRule(Describable<?> plugin) {
        executeActionRule(loadObjectType(), plugin, false, List.of());
    }

    @Override
    public void executeActionRule(Describable<?> plugin, boolean deleteConfirmed) {
        executeActionRule(loadObjectType(), plugin, deleteConfirmed, List.of());
    }

    /**
     * 带上「本 Action 全部对象规则引用的对象类型」执行。
     *
     * <p>多于一个对象类型时，参数池按 {@code <objectTypeReference>_<属性名>} 做了命名空间隔离
     * （见 {@link #getEnumableCandidateSet(ActionParameters, List)}），执行期必须把自己命名空间下的
     * 字段还原成属性名，否则 UPDATE/INSERT 会拿 {@code user_id} 当列名。单对象类型时本方法
     * 的行为与只带 {@code plugin} 的旧入口完全一致（不入命名空间）。
     */
    @Override
    public void executeActionRule(Describable<?> plugin, boolean deleteConfirmed, List<String> objectTypeRefs) {
        executeActionRule(loadObjectType(), plugin, deleteConfirmed, objectTypeRefs);
    }

    private void executeActionRule(OntologyObjectType objectType, Describable<?> plugin,
                                   boolean deleteConfirmed, List<String> objectTypeRefs) {
        ObjectTypeBinding dataSourceBinding = objectType.getDataSourceBinding();
        if (!dataSourceBinding.hasBound()) {
            throw new IllegalStateException("objectType:" + objectType.getName() + " has not bind dataSource");
        }
        DataSourceFactory dataSource = dataSourceBinding.getDataSource();
        // ObjectType 逻辑名不一定等于物理表名：DataSourceBinding#physicalTableName 允许两者不同，
        // 留空时才回落到 ObjectType 名（见 resolvePhysicalTableName）
        final String physicalTableName = dataSourceBinding instanceof DataSourceBinding binding
                ? binding.resolvePhysicalTableName(objectType.getName())
                : objectType.getName();
        // 表单只读一次：三条规则分支共用同一份「属性名 → 值」，且已经把别的对象类型的字段摘掉
        final Map<String, Object> values = scopedPropertyVals(plugin, objectType, objectTypeRefs);

        dataSource.visitFirstConnection((conn) -> {
            objectRule.accept(new AdapterRuleVisitor(true) {
                @Override
                public void visit(CreateObjectRule rule) {

                    insertObject(conn, dataSource, physicalTableName, values);

                }

                @Override
                public void visit(ModifyObjectRule rule) {
                    try {
                        updateObject(conn, dataSource, physicalTableName, objectType
                                , values, rule.ridParameterName);
                    } catch (SQLException e) {
                        throw new RuntimeException("failed to modify object in table:" + physicalTableName, e);
                    }
                }

                @Override
                public void visit(DeleteObjectRule rule) {
                    // 确认标记有两个来源：Workshop 模态框的危险按钮（deleteConfirmed 参数），
                    // 以及历史上可能混进参数池的同名表单字段，任一为 true 即视为已确认
                    boolean confirmed = deleteConfirmed
                            || Boolean.parseBoolean(String.valueOf(values.get(KEY_DELETE_CONFIRMED)));
                    if (Boolean.TRUE.equals(rule.confirmationRequired) && !confirmed) {
                        throw new IllegalStateException("删除操作需要用户确认：请在 Action 参数中传入 "
                                + KEY_DELETE_CONFIRMED + "=true");
                    }
                    if (Boolean.TRUE.equals(rule.cascadeDelete)) {
                        // 关联关系不是本表的一部分（链接是独立的 OntologyLinker 实体），
                        // 在此静默忽略会导致悬挂链接，宁可显式拒绝
                        throw new UnsupportedOperationException(
                                "cascadeDelete is not supported for dataSource bound objectType:"
                                        + objectType.getName());
                    }
                    try {
                        deleteObject(conn, dataSource, physicalTableName, objectType
                                , values, rule.ridParameterName);
                    } catch (SQLException e) {
                        throw new RuntimeException("failed to delete object in table:" + physicalTableName, e);
                    }
                }
            });
        });
    }

    /**
     * 按对象身份（rid）从物理表加载完整对象，供 Workshop 运行时表单预填。
     *
     * <p>当前执行语义是「表单里有的字段全部 SET」（见 {@link #updateObject}），因此只预填主键
     * 是不够的 —— 未预填的字段会被当成空值写回。这里按对象模型的全部属性做一次
     * {@code SELECT ... WHERE <身份列> = ?}。
     *
     * <p>身份列的解析与 {@link #resolveRowIdentity} 完全同口径：声明了主键用主键列，
     * 否则退回规则上的 {@code ridParameterName}。
     *
     * @return 对象属性 → 值；没有匹配行时返回 null
     */
    public Map<String, Object> loadObjectByRid(String domain, String ridParameterName, Object rid) {
        return loadObjectByRid(domain, ridParameterName, rid, "");
    }

    /**
     * 带参数命名空间的版本。
     *
     * <p>返回的 key 必须与运行时表单的字段名一致，否则前端预填不上：多对象类型 Action 的
     * 字段名是 {@code <objectTypeReference>_<属性名>}，故这里把 key 一并加上前缀。
     *
     * @param namespacePrefix 见 {@link #namespacePrefix(String, List)}，空串表示不进命名空间
     */
    public Map<String, Object> loadObjectByRid(String domain, String ridParameterName, Object rid,
                                               String namespacePrefix) {
        OntologyObjectType objectType = loadObjectType(domain);
        ObjectTypeBinding dataSourceBinding = objectType.getDataSourceBinding();
        if (dataSourceBinding == null || !dataSourceBinding.hasBound()) {
            throw new IllegalStateException("objectType:" + objectType.getName() + " has not bind dataSource");
        }
        DataSourceFactory dataSource = dataSourceBinding.getDataSource();
        final String physicalTableName = dataSourceBinding instanceof DataSourceBinding binding
                ? binding.resolvePhysicalTableName(objectType.getName())
                : objectType.getName();
        final String identityColumn = resolveIdentityParameterName(objectType, ridParameterName);
        final List<String> cols = objectType.getCols().stream()
                .map(OntologyProperty::getName)
                .collect(Collectors.toList());
        if (cols.isEmpty()) {
            throw new IllegalStateException("objectType:" + objectType.getName() + " has no property");
        }

        StringBuilder sql = new StringBuilder("SELECT ");
        sql.append(cols.stream().map(dataSource::getEscapedEntity).collect(Collectors.joining(",")));
        sql.append(" FROM ").append(dataSource.getEscapedEntity(physicalTableName));
        sql.append(" WHERE ").append(dataSource.getEscapedEntity(identityColumn)).append("=?");

        final RowMap[] result = new RowMap[1];
        dataSource.visitFirstConnection((conn) -> {
            try (PreparedStatement statement = conn.preparedStatement(sql.toString())) {
                statement.setObject(1, rid);
                try (ResultSet rs = statement.executeQuery()) {
                    if (rs.next()) {
                        RowMap row = new RowMap();
                        for (String col : cols) {
                            String key = StringUtils.isEmpty(namespacePrefix) ? col : namespacePrefix + col;
                            row.put(key, normalizeJdbcValue(rs.getObject(col)));
                        }
                        result[0] = row;
                    }
                }
            } catch (SQLException e) {
                throw new RuntimeException("failed to load object from table:" + physicalTableName, e);
            }
        });
        return result[0];
    }

    /**
     * 把 JDBC 的日期/时间类型转成前端日期控件能直接回填的字符串。
     *
     * <p>不转的话 fastjson 会把 {@link java.util.Date} 序列化成 epoch 数字，日期控件拿到后
     * 要么显示 1970、要么空白，看起来像「对象没有值」。
     */
    private static Object normalizeJdbcValue(Object value) {
        if (value instanceof java.sql.Timestamp timestamp) {
            return new SimpleDateFormat(DATE_TIME_PATTERN).format(timestamp);
        }
        if (value instanceof java.sql.Date date) {
            return new SimpleDateFormat(DATE_PATTERN).format(date);
        }
        if (value instanceof LocalDateTime dateTime) {
            return dateTime.format(DateTimeFormatter.ofPattern(DATE_TIME_PATTERN));
        }
        if (value instanceof LocalDate date) {
            return date.format(DateTimeFormatter.ofPattern(DATE_PATTERN));
        }
        return value;
    }

    private static final String DATE_PATTERN = "yyyy-MM-dd";
    private static final String DATE_TIME_PATTERN = "yyyy-MM-dd HH:mm:ss";

    /**
     * 「需二次确认」参数名，与 {@link DeleteObjectRule} 的约定保持一致。
     */
    private static final String KEY_DELETE_CONFIRMED = "deleteConfirmed";


    /**
     * 定位一行的条件：列名、取值的参数名、取值。
     *
     * <p>对象类型声明了主键时以主键列为准，否则退回规则上配置的 {@code ridParameterName}
     * （既作为参数名也作为列名）。
     */
    private record RowIdentity(String column, String parameterName, Object value) {
    }

    /**
     * 对象身份参数名（供前端预填选中对象 rid）。返回「运行时会用来定位目标对象」的表单字段名：
     * 对象类型声明主键时即主键列名（参数池默认按属性名生成，见 {@link ActionParameterMapper#create}），
     * 否则退回规则上的 {@code ridParameterName}。
     *
     * <p>执行期仍由 {@link #resolveRowIdentity} 做精确解析，此处仅为预填提供字段名。
     */
    public static String resolveIdentityParameterName(OntologyObjectType objectType, String ridParameterName) {
        Optional<OntologyProperty> pk = objectType.hasDisablePK() ? Optional.empty() : objectType.getPk();
        if (pk.isPresent()) {
            return pk.get().getName();
        }
        return Objects.requireNonNull(ridParameterName, "ridParameterName can not be null");
    }

    /**
     * 带命名空间的版本：多对象类型 Action 里表单字段名是 {@code <objectTypeReference>_<属性名>}。
     *
     * @param namespacePrefix {@link #namespacePrefix(String, List)} 的结果，空表示本 Action 只有
     *                        一个对象类型，字段名原样返回
     */
    public static String resolveIdentityParameterName(OntologyObjectType objectType, String ridParameterName,
                                                      String namespacePrefix) {
        String name = resolveIdentityParameterName(objectType, ridParameterName);
        return StringUtils.isEmpty(namespacePrefix) ? name : namespacePrefix + name;
    }

    /**
     * 本 Action 里出现过的全部对象类型引用（去重、保序、忽略空值）。
     */
    public static List<String> objectTypeReferences(List<? extends ActionType> actionTypes) {
        if (CollectionUtils.isEmpty(actionTypes)) {
            return List.of();
        }
        return actionTypes.stream()
                .filter(ObjectActionType.class::isInstance)
                .map(ObjectActionType.class::cast)
                .map((objectRule) -> objectRule.objectTypeReference)
                .filter(StringUtils::isNotEmpty)
                .distinct()
                .toList();
    }

    /**
     * 本规则在参数池 / 运行时表单里的命名空间前缀（含分隔符），单对象类型 Action 返回空串。
     *
     * <p>Palantir 允许一个 Action 组合指向不同对象类型的规则；这些规则共用一个参数池，
     * 而参数名同时是生成的表单字段名和 SQL 列名。两个对象类型都有 {@code id} 时参数池会
     * 撞成一行、执行时会互相写对方的列，因此<b>只有多于一个对象类型</b>时才加前缀
     * {@code <objectTypeReference>_}；只有一个对象类型时保持原样，存量 Action 的参数池、
     * 生成的 groovy 与落盘文件逐字节不变。
     */
    public String namespacePrefix(List<String> allObjectTypeRefs) {
        return namespacePrefix(this.objectTypeReference, allObjectTypeRefs);
    }

    /**
     * @param objectTypeRef     规则引用的对象类型
     * @param allObjectTypeRefs 本 Action 全部对象规则引用的对象类型，见 {@link #objectTypeReferences(List)}
     */
    public static String namespacePrefix(String objectTypeRef, List<String> allObjectTypeRefs) {
        if (StringUtils.isEmpty(objectTypeRef) || allObjectTypeRefs == null || allObjectTypeRefs.size() <= 1) {
            return "";
        }
        return qualifyPrefix(objectTypeRef);
    }

    /**
     * 对象类型名 → 合法标识符前缀。对象类型名可以含中文/连字符，而前缀要与属性名拼成
     * groovy 字段名（受 {@code identity_strict} 与 Java 标识符规则约束）。
     */
    static String qualifyPrefix(String objectTypeRef) {
        String sanitized = objectTypeRef.replaceAll("[^_A-Za-z0-9]", "_");
        if (sanitized.isEmpty() || Character.isDigit(sanitized.charAt(0))) {
            sanitized = "_" + sanitized;
        }
        return sanitized + NAMESPACE_SEPARATOR;
    }

    /**
     * 把运行时表单里属于本规则的字段还原成「属性名 → 值」。
     *
     * <p>单对象类型 Action 直接返回全部字段（存量行为）；多对象类型时只保留
     * {@code <本规则前缀><属性名>} 的字段并去掉前缀，从而：
     * <ul>
     *   <li>不会拿别的对象类型的字段当本表列名（否则 INSERT/UPDATE 直接报未知列）；</li>
     *   <li>只写对象类型上真实存在的属性，参数池里混入的别类参数（链接 RID 等）不会进 SQL。</li>
     * </ul>
     */
    Map<String, Object> scopedPropertyVals(Describable<?> plugin, OntologyObjectType objectType,
                                           List<String> allObjectTypeRefs) {
        Set<String> cols = objectType.getCols().stream()
                .map(OntologyProperty::getName)
                .collect(Collectors.toSet());
        Map<String, Object> allValues = getPluginPropertyVals(plugin);
        Map<String, Object> scoped = scopeValues(allValues, cols, allObjectTypeRefs);
        return mergePropertyMappings(objectRule, scoped, allValues, resolveColTypes(objectType));
    }

    /**
     * 把规则的 {@code propertyMappings} 解析结果合并进本命名空间的值（fill-gap 语义）。
     *
     * <p>表单已提供的字段优先，映射只补<b>缺失</b>的键——多对象类型 Action 里跨命名空间引用
     * 正是这种场景：副表 {@code report_id} 没进参数池，映射行指向主表参数
     * {@code <主表>_<report_id>}，因此参数查找必须用<b>未裁剪的全量表单值</b>（{@code allValues}），
     * 不能用 {@code scoped}。不能反过来让映射覆盖表单值：存量配置里主表 {@code report_date}
     * 既是必填表单参数又有 {@code CurrentTimeValueSource} 映射行，映射权威会把用户填的日期
     * 冲成 {@code now()}。
     *
     * <p>{@link DeleteObjectRule} 没有 {@code propertyMappings} 字段，原样返回；
     * {@link ModifyObjectRule} 同样受益——合并发生在 visitor 之前，
     * {@code resolveRowIdentity} 也能吃到映射供给的 rid 参数。
     *
     * @param colTypes 本规则对象类型的「属性名 → 类型」（键集合即属性名集合），越界的陈旧映射行
     *                 丢弃，防止拼出未知列的 SQL；类型同时供时间语义的来源产出正确 JDBC 类型
     *                 （见 {@code ValueSourceConfig#resolveValue}）
     */
    static Map<String, Object> mergePropertyMappings(OntologyActionRule rule,
                                                     Map<String, Object> scoped,
                                                     Map<String, Object> allValues,
                                                     Map<String, OntologyType> colTypes) {
        List<PropertyMappingConfig> mappings = null;
        if (rule instanceof CreateObjectRule createRule) {
            mappings = createRule.propertyMappings;
        } else if (rule instanceof ModifyObjectRule modifyRule) {
            mappings = modifyRule.propertyMappings;
        }
        if (mappings == null) {
            return scoped;
        }
        for (Map.Entry<String, Object> entry
                : OntologyActionRule.buildMappedValues(mappings, allValues, colTypes).entrySet()) {
            // 解析为 null 的行（如参数不存在）buildMappedValues 本就跳过；
            // 只补 !containsKey 的键：表单字段存在但值为 null 时保持 null（"表单是唯一真相"现行语义）
            if (colTypes.containsKey(entry.getKey()) && !scoped.containsKey(entry.getKey())) {
                scoped.put(entry.getKey(), entry.getValue());
            }
        }
        return scoped;
    }

    /**
     * 有序规则上按「属性名 → 首个贡献该属性的规则的命名空间参数名」解析 canonical 参数。
     *
     * <p>参数池 / 映射默认行的跨规则联动共用这一处事实源：副表与主表都有 {@code report_id} 时
     * canonical 参数是主表的 {@code <主表前缀>report_id}——参数池候选据此标注「已由 … 提供」
     * （{@code ActionParameterConfig} 聚合处）、映射默认行据此预置取值来源
     * （{@code PropertyMappingConfig} 候选处）。
     *
     * <p>名字相同 ≠ 语义相同：联动产出的是<b>可覆盖的默认</b>而非硬约束，用户仍可在参数池
     * 勾选本命名空间的独立参数——此时映射行自然失效，fill-gap 语义保证表单值优先。
     *
     * <p>每条规则的属性列表经 {@link #loadObjectType()} 现场加载，与候选生成同一线程上下文
     * （调用方都在 descriptor 的 {@code getEnumableCandidateSet} 请求线程内）；半配置状态
     * （对象类型未选 / 已删、上下文缺失）丢该规则的贡献但不抛，绝不阻断候选生成。
     *
     * @return 属性名 → canonical 参数名（首个贡献规则的前缀 + 属性名）。单对象类型 Action
     * 前缀为空串、退化为裸属性名，与存量默认行逐字一致
     */
    public static Map<String, String> resolveCanonicalParams(List<ObjectActionType> orderedRules,
                                                             List<String> allObjectTypeRefs) {
        Map<String, String> canonical = new LinkedHashMap<>();
        if (orderedRules == null) {
            return canonical;
        }
        for (ObjectActionType rule : orderedRules) {
            if (rule == null || StringUtils.isEmpty(rule.objectTypeReference)) {
                continue;
            }
            String prefix = namespacePrefix(rule.objectTypeReference, allObjectTypeRefs);
            try {
                OntologyObjectType objectType = rule.loadObjectType();
                if (objectType == null) {
                    continue;
                }
                for (OntologyProperty col : objectType.getCols()) {
                    canonical.putIfAbsent(col.getName(), prefix + col.getName());
                }
            } catch (RuntimeException e) {
                // 半配置状态只丢这一条规则的贡献，其余规则照常参与
            }
        }
        return canonical;
    }

    private static final Logger LOG = LoggerFactory.getLogger(ObjectActionType.class);

    /**
     * 执行前覆盖校验（2026/11/24，执行链最后一道防线，调用点
     * {@code DefaultOntologyAction#executeActionTypes} 头部、事务开启前 fail-fast）。
     *
     * <ul>
     *   <li><b>映射行 typo 硬拦截</b>（Create 与 Modify 都参与）：映射行 {@code parameterRef}
     *       在运行期表单值里解析不到同名参数 → IllegalStateException 点名 {@code target ← parameterRef}
     *       ——参数池手工时代的典型 typo 面正是这里（共享字段手工引用主表前缀参数名，写错一位
     *       就静默落 null、INSERT 缺列）；执行期 {@code resolveValue} 查的就是这张 map，
     *       现在解析不到的将来也永远解析不到。</li>
     *   <li><b>CREATE 未覆盖属性告警不阻断</b>：cols − (本命名空间表单参数 ∪ 映射 target)
     *       非空 → WARN 点名。部分列插入是合法业务形态（先落主键列、其余列由后续流程补齐），
     *       不做硬失败；Modify 的「表单有啥 SET 啥」现行语义不变，不参与覆盖告警。</li>
     * </ul>
     *
     * <p>半配置规则（{@code loadObjectType} 失败）跳过覆盖告警但 typo 检查照做；
     * Delete / 函数规则无映射行，天然不参与。
     */
    public static void validatePropertyCoverage(List<ActionType> actionTypes,
                                                Describable<?> previewInstance,
                                                List<String> allObjectTypeRefs) {
        if (!hasPropertyMappings(actionTypes)) {
            // 全部规则都无映射行（Delete / 函数规则等）：不读表单值直接放行——
            // 事务契约测试的 mock ActionType / previewInstance 也在这条短路里天然跳过
            return;
        }
        validatePropertyCoverage(actionTypes, getPluginPropertyVals(previewInstance), allObjectTypeRefs);
    }

    /**
     * 是否存在带映射行的 Create/Modify 规则（无则校验整体跳过）。
     */
    private static boolean hasPropertyMappings(List<ActionType> actionTypes) {
        if (actionTypes == null) {
            return false;
        }
        for (ActionType actionType : actionTypes) {
            if (actionType instanceof ObjectActionType objectRule) {
                OntologyActionRule rule = objectRule.getRule();
                if (rule instanceof CreateObjectRule createRule && createRule.propertyMappings != null) {
                    return true;
                }
                if (rule instanceof ModifyObjectRule modifyRule && modifyRule.propertyMappings != null) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * 覆盖校验纯静态内核（与表单实例解耦，便于单测）。
     */
    static void validatePropertyCoverage(List<ActionType> actionTypes,
                                         Map<String, Object> allValues,
                                         List<String> allObjectTypeRefs) {
        if (actionTypes == null || allValues == null) {
            return;
        }
        for (ActionType actionType : actionTypes) {
            if (!(actionType instanceof ObjectActionType objectRule)) {
                continue;
            }
            OntologyActionRule rule = objectRule.getRule();
            List<PropertyMappingConfig> mappings = null;
            if (rule instanceof CreateObjectRule createRule) {
                mappings = createRule.propertyMappings;
            } else if (rule instanceof ModifyObjectRule modifyRule) {
                mappings = modifyRule.propertyMappings;
            }
            if (mappings == null) {
                continue;
            }
            // typo 硬拦截：表单字段名 = 参数池 parameterId，执行期 resolveValue 查的正是这张 map
            for (PropertyMappingConfig mapping : mappings) {
                if (mapping.source instanceof FromParameterValueSource paramSource
                        && !allValues.containsKey(paramSource.parameterRef)) {
                    throw new IllegalStateException("mapped parameter not found (check the parameter name spelling, no such parameter in the action parameter pool): "
                            + mapping.target + " <- " + paramSource.parameterRef
                            + ", objectType:" + objectRule.objectTypeReference);
                }
            }
            if (!(rule instanceof CreateObjectRule)) {
                continue;
            }
            OntologyObjectType objectType;
            try {
                objectType = objectRule.loadObjectType();
            } catch (RuntimeException e) {
                // 半配置规则跳过覆盖告警（typo 检查已照做；执行期会报对象类型加载失败）
                continue;
            }
            if (objectType == null || objectType.getCols() == null) {
                continue;
            }
            String prefix = namespacePrefix(objectRule.objectTypeReference, allObjectTypeRefs);
            Set<String> cols = objectType.getCols().stream()
                    .map(OntologyProperty::getName).collect(Collectors.toSet());
            Set<String> covered = new java.util.HashSet<>();
            for (String key : allValues.keySet()) {
                if (key.startsWith(prefix) && cols.contains(key.substring(prefix.length()))) {
                    covered.add(key.substring(prefix.length()));
                }
            }
            for (PropertyMappingConfig mapping : mappings) {
                if (cols.contains(mapping.target)) {
                    covered.add(mapping.target);
                }
            }
            Set<String> uncovered = new java.util.TreeSet<>(cols);
            uncovered.removeAll(covered);
            if (!uncovered.isEmpty()) {
                LOG.warn("Action 规则[{}] 对象类型[{}] 存在未覆盖属性（INSERT 将不含这些列，部分列插入属合法形态可忽略）: {}",
                        objectRule.ruleName, objectRule.objectTypeReference, uncovered);
            }
        }
    }

    /**
     * {@link #scopedPropertyVals} 的纯函数内核（与表单 / 对象类型解耦，便于单测）。
     *
     * @param allValues         运行期表单的「字段名 → 值」
     * @param cols              本对象类型的属性名集合
     * @param allObjectTypeRefs 本 Action 全部对象类型引用，见 {@link #objectTypeReferences(List)}
     */
    Map<String, Object> scopeValues(Map<String, Object> allValues, Set<String> cols,
                                    List<String> allObjectTypeRefs) {
        String prefix = namespacePrefix(this.objectTypeReference, allObjectTypeRefs);
        if (StringUtils.isEmpty(prefix)) {
            return allValues;
        }
        Map<String, Object> scoped = new LinkedHashMap<>();
        for (Map.Entry<String, Object> entry : allValues.entrySet()) {
            String key = entry.getKey();
            if (key.startsWith(prefix)) {
                String prop = key.substring(prefix.length());
                if (cols.contains(prop)) {
                    scoped.put(prop, entry.getValue());
                }
            }
        }
        return scoped;
    }

    /**
     * 解析 UPDATE / DELETE 的 WHERE 条件。
     *
     * <p>优先使用对象类型的主键列：规则上的 {@code ridParameterName} 是「哪个 Action 参数携带
     * 目标对象标识」，而真正写进 SQL 的列名应当来自对象模型。两者默认同名，但用户可能把参数
     * 改名，故这里分开处理；参数名在表单里不存在时回落到主键列本身同名的参数（参数池默认按
     * 属性名生成，见 {@link ActionParameterMapper#create}）。
     */
    private static RowIdentity resolveRowIdentity(OntologyObjectType objectType, String ridParameterName,
                                                  Map<String, Object> values) {
        // 显式声明「没有主键」时忽略属性上可能残留的 pk 标记
        Optional<OntologyProperty> pk = objectType.hasDisablePK() ? Optional.empty() : objectType.getPk();
        String column;
        String parameterName;
        if (pk.isPresent()) {
            column = pk.get().getName();
            parameterName = values.containsKey(ridParameterName) ? ridParameterName : column;
        } else {
            column = Objects.requireNonNull(ridParameterName, "ridParameterName can not be null");
            parameterName = ridParameterName;
        }
        Object value = values.get(parameterName);
        if (value == null) {
            throw new IllegalArgumentException("can not find object identity parameter '" + parameterName
                    + "' for objectType:" + objectType.getName());
        }
        return new RowIdentity(column, parameterName, value);
    }

    /**
     * 把运行时表单实例（{@code ActionFormPreview}）的字段读成「列名 → 值」。
     *
     * <p>表单字段名就是 Action 参数名（见 {@code ActionFormDescriptorBuilder}），而对象类型的
     * 参数默认按属性名生成（见 {@link ActionParameterMapper#create}），因此字段名可直接作为列名。
     * {@code getVal(false, ...)} 的 {@code false} 表示取原始值而非前端序列化形态。
     *
     * <p>包级可见是为了让同包的 {@link LinkActionExecutor} 复用同一套「运行时表单 → 参数值」
     * 读取口径；链接规则与对象规则的表单来源相同。
     */
    static Map<String, Object> getPluginPropertyVals(Describable<?> plugin) {
        Descriptor<?> desc = Objects.requireNonNull(plugin.getDescriptor()
                , "plugin descriptor can not be null:" + plugin.getClass().getName());
        // 用 LinkedHashMap 保住字段声明顺序，保证 SQL 列序与参数绑定顺序一致
        Map<String, Object> values = new LinkedHashMap<>();
        for (Map.Entry<String, IPropertyType> entry : desc.getPropertyTypes().entrySet()) {
            PropertyType pp = (PropertyType) entry.getValue();
            values.put(entry.getKey(), pp.getVal(false, plugin));
        }
        return values;
    }

    /**
     * 「属性名 → 类型」。共享属性的类型引用（{@code SharedPropertyTypeRef}）要查 plugin store
     * 现场解析，半配置状态可能抛——逐列 try/catch 记 null（取值处按「类型未知」保守处理），
     * 不让类型解析阻断 SQL 执行。
     */
    static Map<String, OntologyType> resolveColTypes(OntologyObjectType objectType) {
        Map<String, OntologyType> colTypes = new HashMap<>();
        for (OntologyProperty col : objectType.getCols()) {
            try {
                colTypes.put(col.getName(), col.parseOntologyType());
            } catch (Throwable e) {
                colTypes.put(col.getName(), null);
            }
        }
        return colTypes;
    }

    /**
     * 组装并执行一条 INSERT：列名与值一一取自 {@code values}，值通过
     * {@link PreparedStatement#setObject(int, Object)} 绑定，不做字符串拼接。
     */
    private static void insertObject(JDBCConnection conn, IDBReservedKeys dataSource,
                                     String physicalTableName, Map<String, Object> values) {
        if (values.isEmpty()) {
            throw new IllegalStateException("no action parameter can be inserted into table:" + physicalTableName);
        }
        String columns = values.keySet().stream()
                .map(dataSource::getEscapedEntity)
                .collect(Collectors.joining(","));
        String placeholders = String.join(",", Collections.nCopies(values.size(), "?"));
        String insertSQL = "INSERT INTO " + dataSource.getEscapedEntity(physicalTableName)
                + " (" + columns + ") VALUES (" + placeholders + ")";

        try (PreparedStatement statement = conn.preparedStatement(insertSQL)) {
            int parameterIndex = 1;
            for (Object value : values.values()) {
                statement.setObject(parameterIndex++, value);
            }
            statement.executeUpdate();
        } catch (Exception e) {
            throw new RuntimeException("insert SQL:" + insertSQL, e);
        }
    }

    /**
     * 组装并执行一条 UPDATE：所有表单字段（定位列 / 定位参数除外）作为 SET 赋值，
     * {@link RowIdentity} 作为 WHERE 条件。
     *
     * <p>不校验 {@code executeUpdate()} 的返回值：MySQL 默认返回的是<b>实际变更</b>的行数，
     * 「找到了对象但新旧值相同」同样会返回 0，据此报「对象不存在」会误伤。
     */
    private static void updateObject(JDBCConnection conn, IDBReservedKeys dataSource, String physicalTableName,
                                     OntologyObjectType objectType, Map<String, Object> values,
                                     String ridParameterName) throws SQLException {
        RowIdentity identity = resolveRowIdentity(objectType, ridParameterName, values);
        // 定位列不参与 SET：既避免把主键改掉，也避免把非列的参数（如独立的 rid 参数）写进 SET
        Map<String, Object> setValues = new LinkedHashMap<>(values);
        setValues.remove(identity.column());
        setValues.remove(identity.parameterName());
        if (setValues.isEmpty()) {
            throw new IllegalStateException("no property to update for objectType:" + objectType.getName());
        }

        String setClause = setValues.keySet().stream()
                .map((col) -> dataSource.getEscapedEntity(col) + "=?")
                .collect(Collectors.joining(","));
        String updateSQL = "UPDATE " + dataSource.getEscapedEntity(physicalTableName)
                + " SET " + setClause
                + " WHERE " + dataSource.getEscapedEntity(identity.column()) + "=?";

        try (PreparedStatement statement = conn.preparedStatement(updateSQL)) {
            int parameterIndex = 1;
            for (Object value : setValues.values()) {
                statement.setObject(parameterIndex++, value);
            }
            statement.setObject(parameterIndex, identity.value());
            statement.executeUpdate();
        }
    }

    /**
     * 组装并执行一条 DELETE。
     *
     * <p>与 UPDATE 不同，这里必须校验影响行数：DELETE 返回 0 只可能是「没有匹配的行」——
     * 静默成功会让用户以为对象已被删除。
     */
    private static void deleteObject(JDBCConnection conn, IDBReservedKeys dataSource, String physicalTableName,
                                     OntologyObjectType objectType, Map<String, Object> values,
                                     String ridParameterName) throws SQLException {
        RowIdentity identity = resolveRowIdentity(objectType, ridParameterName, values);
        String deleteSQL = "DELETE FROM " + dataSource.getEscapedEntity(physicalTableName)
                + " WHERE " + dataSource.getEscapedEntity(identity.column()) + "=?";

        try (PreparedStatement statement = conn.preparedStatement(deleteSQL)) {
            statement.setObject(1, identity.value());
            if (statement.executeUpdate() == 0) {
                throw new IllegalStateException("no object found to delete in table:" + physicalTableName
                        + ", " + identity.column() + "=" + identity.value());
            }
        }
    }

    @Override
    public List<Pair<Option, ActionParameterConfig>> getEnumableCandidateSet(ActionParameters actionParams) {
        return getEnumableCandidateSet(actionParams, List.of());
    }

    /**
     * @param allObjectTypeRefs 本 Action 全部对象规则引用的对象类型，见 {@link #objectTypeReferences(List)}。
     *                          多于一个时本规则产出的参数名带 {@code <objectTypeReference>_} 前缀（见
     *                          {@link #namespacePrefix(String, List)}），否则与旧行为逐字一致。
     */
    @Override
    public List<Pair<Option, ActionParameterConfig>> getEnumableCandidateSet(ActionParameters actionParams,
                                                                             List<String> allObjectTypeRefs) {
        List<Pair<Option, ActionParameterConfig>> result = Lists.newArrayList();
        // 加载对象类型及其属性列表
        OntologyObjectType objectType = this.loadObjectType();
        List<OntologyProperty> cols = objectType.getCols();
        final String prefix = namespacePrefix(this.objectTypeReference, allObjectTypeRefs);

        // 提取当前参数池里已选中的 id 集合
        Set<String> selected = new java.util.HashSet<>();
        List<ActionParameterConfig> parameters = actionParams.parameters;
        if (CollectionUtils.isNotEmpty(parameters)) {
            selected = parameters.stream()
                    .map(ActionParameterConfig::identityValue)
                    .collect(java.util.stream.Collectors.toSet());
        }
        // 值类型与属性是分开存储的、属性上只有值类型的名字，故整表加载一次，循环内按名字复用
        Map<String, OntologyValueType> valueTypes =
                ActionParameterMapper.loadValueTypes(OntologyPluginMeta.createPluginMeta().getDomain());
        // 为每个属性创建候选项
        int order = 0;
        for (OntologyProperty col : cols) {
            ActionParameterConfig config = ActionParameterMapper.create(col, valueTypes);
            if (StringUtils.isNotEmpty(prefix)) {
                // 参数名即表单字段名，多对象类型时必须带命名空间前缀；displayName 留空，
                // 由 ActionParameterConfig#label() 回落到带前缀的 parameterId，避免两个对象类型的
                // 同名属性在参数池表格里显示成两行一模一样的「id」
                config.parameterId = prefix + col.getName();
                config.displayName = StringUtils.defaultIfBlank(StringUtils.left(col.description, 6), col.getName());
            }
            config.order = order++;
            result.add(Pair.of(
                    // val 必须等于 config.identityValue()（即带前缀的 parameterId），
                    // generate_target_instance 阶段是拿前端回送的 val 对 candidate 的
                    // identityValue() 做 contains 过滤的，val 不一致会整批静默过滤成空
                    new Option(col.getName(), config.parameterId).setChecked(selected.contains(config.parameterId)),
                    config
            ));
        }

        return result;
    }


    @Override
    public RuleGroup getRuleGroup() {
        return RuleGroup.ONTOLOGY_OBJECT;
    }

    @Override
    public List<RuleType> getAllowedRuleTypes() {
        return List.of(
                RuleType.CREATE_OBJECT,
                RuleType.MODIFY_OBJECT,
                RuleType.DELETE_OBJECT,
                RuleType.CREATE_OR_MODIFY_OBJECT
        );
    }

    @Override
    public OntologyActionRule getRule() {
        return objectRule;
    }

    @TISExtension
    public static class DefaultDescriptor extends BaseActionTypeDescriptor {

        public DefaultDescriptor() {
            super();
            this.registerSelectOptions(KEY_OBJECT_TYPE_REFERENCE, LinkReference::getObjectTypeOpts);
            // 选中对象类型后，规则里的「RID 参数名」候选 = 该对象类型的全部属性（主键带注释）
            this.valueChangePipe(KEY_OBJECT_TYPE_REFERENCE, KEY_OBJECT_RULE + "." + ModifyObjectRule.KEY_RID_PARAMETER_NAME)
                    .render((meta, params) -> {
                        String objType = params.getString(KEY_OBJECT_TYPE_REFERENCE);
                        if (StringUtils.isEmpty(objType)) {
                            return List.of();
                        }
                        try {
                            OntologyObjectType objectType = Ontology.loadObjectTypeDetail(
                                    OntologyPluginMeta.createPluginMeta(meta).getDomain(), objType);
                            return objectType == null ? List.of() : objectType.getColOpts();
                        } catch (RuntimeException e) {
                            // 渲染期取不到只丢这一组候选，绝不能让整张表单打不开：
                            // RootFormProperties#getInstancePropsJson 会把这里的异常包成 RuntimeException 抛给请求
                            return List.of();
                        }
                    });
        }

        @Override
        public RuleGroup getRuleGroup() {
            return RuleGroup.ONTOLOGY_OBJECT;
        }

//        @Override
//        public String getDisplayName() {
//            return "Object";
//        }
    }
}