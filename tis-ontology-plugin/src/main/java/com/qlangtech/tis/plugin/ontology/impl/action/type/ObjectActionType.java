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
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.IDBReservedKeys;
import com.qlangtech.tis.plugin.ds.JDBCConnection;
import com.qlangtech.tis.plugin.ontology.Ontology;
import com.qlangtech.tis.plugin.ontology.OntologyObjectType;
import com.qlangtech.tis.plugin.ontology.OntologyProperty;
import com.qlangtech.tis.plugin.ontology.OntologyValueType;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.plugin.ontology.impl.action.ActionParameterConfig;
import com.qlangtech.tis.plugin.ontology.impl.action.ActionParameterMapper;
import com.qlangtech.tis.plugin.ontology.impl.action.ActionParameters;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.AdapterRuleVisitor;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.CreateObjectRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.DeleteObjectRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.ModifyObjectRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.OntologyActionRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.RuleGroup;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.RuleType;
import com.qlangtech.tis.plugin.ontology.impl.objtype.DataSourceBinding;
import com.qlangtech.tis.plugin.ontology.impl.objtype.ObjectTypeBinding;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
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

    @FormField(ordinal = 0, type = FormFieldType.ENUM, validate = {Validator.require})
    public String objectTypeReference;

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
        executeActionRule(loadObjectType(), plugin, false);
    }

    @Override
    public void executeActionRule(Describable<?> plugin, boolean deleteConfirmed) {
        executeActionRule(loadObjectType(), plugin, deleteConfirmed);
    }

    private void executeActionRule(OntologyObjectType objectType, Describable<?> plugin,
                                   boolean deleteConfirmed) {
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

        dataSource.visitFirstConnection((conn) -> {
            objectRule.accept(new AdapterRuleVisitor(true) {
                @Override
                public void visit(CreateObjectRule rule) {

                    insertObject(conn, dataSource, physicalTableName, getPluginPropertyVals(plugin));

                }

                @Override
                public void visit(ModifyObjectRule rule) {
                    try {
                        updateObject(conn, dataSource, physicalTableName, objectType
                                , getPluginPropertyVals(plugin), rule.ridParameterName);
                    } catch (SQLException e) {
                        throw new RuntimeException("failed to modify object in table:" + physicalTableName, e);
                    }
                }

                @Override
                public void visit(DeleteObjectRule rule) {
                    Map<String, Object> values = getPluginPropertyVals(plugin);
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
                            row.put(col, normalizeJdbcValue(rs.getObject(col)));
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
     * 「需二次确认」参数名，与 {@link DeleteObjectRule#execute} 的约定保持一致。
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
        List<Pair<Option, ActionParameterConfig>> result = Lists.newArrayList();
        // 加载对象类型及其属性列表
        OntologyObjectType objectType = this.loadObjectType();
        List<OntologyProperty> cols = objectType.getCols();

        // 提取当前已选中的 target 集合
        // OntologyActionRule rule = objectRule;
        java.util.Set<String> selected = new java.util.HashSet<>();
        // List<PropertyMappingConfig> existingMappings = null;
        List<ActionParameterConfig> parameters = actionParams.parameters;

//        if (rule instanceof CreateObjectRule) {
//            existingMappings = ((CreateObjectRule) rule).propertyMappings;
//        } else if (rule instanceof ModifyObjectRule) {
//            existingMappings = ((ModifyObjectRule) rule).propertyMappings;
//        }

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
            config.order = order++;
            result.add(Pair.of(
                    new Option(col.getName()).setChecked(selected.contains(col.getName())),
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