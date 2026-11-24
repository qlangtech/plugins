package com.qlangtech.tis.plugin.ontology.impl.action.type;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.IPropertyType;
import com.qlangtech.tis.extension.impl.PropertyType;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.JDBCConnection;
import com.qlangtech.tis.plugin.ontology.OntologyObjectType;
import com.qlangtech.tis.plugin.ontology.OntologyProperty;
import com.qlangtech.tis.plugin.ontology.OntologyType;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.CreateObjectRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.CurrentTimeValueSource;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.FromParameterValueSource;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.PropertyMappingConfig;
import com.qlangtech.tis.plugin.ontology.impl.objtype.ObjectTypeBinding;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * {@code ObjectActionType#executeActionRule} 的端到端钉子：{@code propertyMappings} 必须
 * <b>真的接线</b>进执行链——纯函数测试盖不住 {@code scopedPropertyVals} 内部是否调用了
 * {@code mergePropertyMappings}（未来重构删掉那一行，纯函数用例依然全绿）。
 *
 * <p>钉住副表场景（主表 + 副表共享 report_id/report_date）：运行时表单里只有副表自己的
 * 两个字段，映射行指向主表命名空间参数，断言最终 INSERT 的列集合 = 副表表单字段 ∪ 映射补入字段。
 * <b>删掉 merge 调用即本用例红</b>——这就是防退化钉子的判定标准。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/24
 */
public class TestObjectActionTypeMappingExecution {

    /**
     * 副表创建规则 + 运行时表单（副表仅 owner_email/remark 两参），映射行把共享字段
     * 接到主表命名空间参数上：INSERT 列集合必须包含映射补入的 report_id/report_date。
     */
    @Test
    public void insertColumnsContainMappedNamespaceFields() throws Exception {
        final String[] capturedSQL = new String[1];
        JDBCConnection conn = Mockito.mock(JDBCConnection.class);
        PreparedStatement insertStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(conn.preparedStatement(Mockito.anyString())).thenAnswer((invocation) -> {
            capturedSQL[0] = invocation.getArgument(0);
            return insertStatement;
        });
        Mockito.when(insertStatement.executeUpdate()).thenReturn(1);

        DataSourceFactory dataSource = Mockito.mock(DataSourceFactory.class);
        Mockito.when(dataSource.getEscapedEntity(Mockito.anyString()))
                .thenAnswer((invocation) -> invocation.getArgument(0));
        Mockito.doAnswer((invocation) -> {
            DataSourceFactory.IConnProcessor processor = invocation.getArgument(0);
            processor.vist(conn);
            return null;
        }).when(dataSource).visitFirstConnection(Mockito.any());

        // 对象类型：副表 4 个属性，数据源绑定已就绪
        OntologyObjectType detailType = Mockito.mock(OntologyObjectType.class);
        Mockito.when(detailType.getName()).thenReturn("chartxy_daily_report_detail");
        // cols(...) 内部会创建并 stub 新 mock，必须先物化再传入 when()——
        // 直接写 thenReturn(cols(...)) 会在未完成的 stubbing 期间嵌套 mock 交互（UnfinishedStubbing）
        List<OntologyProperty> detailCols = cols("report_id", "report_date", "owner_email", "remark");
        Mockito.when(detailType.getCols()).thenReturn(detailCols);
        ObjectTypeBinding binding = Mockito.mock(ObjectTypeBinding.class);
        Mockito.when(binding.hasBound()).thenReturn(true);
        Mockito.when(binding.getDataSource()).thenReturn(dataSource);
        Mockito.when(detailType.getDataSourceBinding()).thenReturn(binding);

        ObjectActionType actionType = Mockito.spy(new ObjectActionType());
        actionType.ruleName = "chartxy_daily_report_detail_create";
        actionType.objectTypeReference = "chartxy_daily_report_detail";
        CreateObjectRule createRule = new CreateObjectRule();
        createRule.propertyMappings = List.of(
                mapping("report_id", "chartxy_daily_report_report_id"),
                mapping("report_date", "chartxy_daily_report_report_date"));
        actionType.objectRule = createRule;
        Mockito.doReturn(detailType).when(actionType).loadObjectType();

        // 运行时表单：副表仅有自己的两个字段（report_id/report_date 不在参数池）
        Map<String, IPropertyType> formFields = new LinkedHashMap<>();
        formField(formFields, "chartxy_daily_report_report_id", 99);
        formField(formFields, "chartxy_daily_report_report_date", "2026-11-24");
        formField(formFields, "chartxy_daily_report_detail_owner_email", "baisui@qlangtech.com");
        formField(formFields, "chartxy_daily_report_detail_remark", "ok");
        Describable<?> formInstance = Mockito.mock(Describable.class);
        Descriptor<?> formDescriptor = Mockito.mock(Descriptor.class);
        // getDescriptor() 的返回类型带通配符捕获，thenReturn 过不了泛型检查，用 doReturn 绕过
        Mockito.doReturn(formDescriptor).when(formInstance).getDescriptor();
        Mockito.when(formDescriptor.getPropertyTypes()).thenReturn(formFields);

        actionType.executeActionRule(formInstance, false,
                List.of("chartxy_daily_report", "chartxy_daily_report_detail"));

        String insertSQL = capturedSQL[0];
        Assert.assertNotNull("INSERT 必须已执行", insertSQL);
        Assert.assertTrue("目标表必须是副表：" + insertSQL,
                insertSQL.startsWith("INSERT INTO chartxy_daily_report_detail"));
        String columnList = insertSQL.substring(insertSQL.indexOf('(') + 1, insertSQL.indexOf(')'));
        Set<String> columns = Arrays.stream(columnList.split(","))
                .map(String::trim).collect(Collectors.toSet());
        Assert.assertEquals("INSERT 列集合 = 副表表单字段 ∪ 映射补入的共享字段：" + insertSQL,
                Set.of("report_id", "report_date", "owner_email", "remark"), columns);
    }

    /**
     * 「当前时间」来源按列类型直出（{@code CurrentTimeValueSource#resolveValue} 接进执行链的钉子）：
     * 单对象类型 Action，DATE 列 {@code report_date} 由表单字符串供给（原样绑定），
     * TIMESTAMP 列 {@code created_at} 不在参数池、由「当前时间」映射行 fill-gap 补入
     * （TIMESTAMP 属性默认行的真实形态）——必须绑定为 {@code java.sql.Timestamp}，
     * 否则 {@code java.sql.Date} 进 DATETIME/TIMESTAMP 列时分秒被补零。
     */
    @Test
    public void bindValuesAdaptedPerColumnOntologyType() throws Exception {
        final String[] capturedSQL = new String[1];
        JDBCConnection conn = Mockito.mock(JDBCConnection.class);
        PreparedStatement insertStatement = Mockito.mock(PreparedStatement.class);
        Mockito.when(conn.preparedStatement(Mockito.anyString())).thenAnswer((invocation) -> {
            capturedSQL[0] = invocation.getArgument(0);
            return insertStatement;
        });
        Mockito.when(insertStatement.executeUpdate()).thenReturn(1);

        DataSourceFactory dataSource = Mockito.mock(DataSourceFactory.class);
        Mockito.when(dataSource.getEscapedEntity(Mockito.anyString()))
                .thenAnswer((invocation) -> invocation.getArgument(0));
        Mockito.doAnswer((invocation) -> {
            DataSourceFactory.IConnProcessor processor = invocation.getArgument(0);
            processor.vist(conn);
            return null;
        }).when(dataSource).visitFirstConnection(Mockito.any());

        OntologyObjectType dailyReport = Mockito.mock(OntologyObjectType.class);
        Mockito.when(dailyReport.getName()).thenReturn("chartxy_daily_report");
        // cols(...) 同理：先物化再传入 when()
        List<OntologyProperty> typedCols = List.of(
                prop("report_id", OntologyType.STRING),
                prop("report_date", OntologyType.DATE),
                prop("created_at", OntologyType.TIMESTAMP));
        Mockito.when(dailyReport.getCols()).thenReturn(typedCols);
        ObjectTypeBinding binding = Mockito.mock(ObjectTypeBinding.class);
        Mockito.when(binding.hasBound()).thenReturn(true);
        Mockito.when(binding.getDataSource()).thenReturn(dataSource);
        Mockito.when(dailyReport.getDataSourceBinding()).thenReturn(binding);

        ObjectActionType actionType = Mockito.spy(new ObjectActionType());
        actionType.ruleName = "chartxy_daily_report_create";
        actionType.objectTypeReference = "chartxy_daily_report";
        CreateObjectRule createRule = new CreateObjectRule();
        PropertyMappingConfig createdAtRow = new PropertyMappingConfig();
        createdAtRow.target = "created_at";
        createdAtRow.source = new CurrentTimeValueSource();
        createRule.propertyMappings = List.of(createdAtRow);
        actionType.objectRule = createRule;
        Mockito.doReturn(dailyReport).when(actionType).loadObjectType();

        Map<String, IPropertyType> formFields = new LinkedHashMap<>();
        formField(formFields, "report_id", "R-1");
        formField(formFields, "report_date", "2026-11-24");
        Describable<?> formInstance = Mockito.mock(Describable.class);
        Descriptor<?> formDescriptor = Mockito.mock(Descriptor.class);
        Mockito.doReturn(formDescriptor).when(formInstance).getDescriptor();
        Mockito.when(formDescriptor.getPropertyTypes()).thenReturn(formFields);

        actionType.executeActionRule(formInstance, false);

        String insertSQL = capturedSQL[0];
        Assert.assertNotNull("INSERT 必须已执行", insertSQL);
        String columnList = insertSQL.substring(insertSQL.indexOf('(') + 1, insertSQL.indexOf(')'));
        Set<String> columns = Arrays.stream(columnList.split(","))
                .map(String::trim).collect(Collectors.toSet());
        Assert.assertEquals("INSERT 列集合 = 表单字段 ∪ 当前时间映射补入字段：" + insertSQL,
                Set.of("report_id", "report_date", "created_at"), columns);

        ArgumentCaptor<Object> bindValues = ArgumentCaptor.forClass(Object.class);
        Mockito.verify(insertStatement, Mockito.times(3)).setObject(Mockito.anyInt(), bindValues.capture());
        List<Object> vals = bindValues.getAllValues();
        Assert.assertEquals("STRING 列原样绑定", "R-1", vals.get(0));
        Assert.assertEquals("DATE 列的表单日期字符串原样绑定（MySQL 现场解析）", "2026-11-24", vals.get(1));
        Assert.assertTrue("TIMESTAMP 列必须绑 java.sql.Timestamp（java.sql.Date 会丢时分秒）：" + vals.get(2),
                vals.get(2) instanceof Timestamp);
    }

    /** 带类型的属性 mock（{@code resolveColTypes} 现场解析列类型依赖此 stub）。 */
    private static OntologyProperty prop(String name, OntologyType type) {
        OntologyProperty property = Mockito.mock(OntologyProperty.class);
        Mockito.when(property.getName()).thenReturn(name);
        Mockito.when(property.parseOntologyType()).thenReturn(type);
        return property;
    }

    private static List<OntologyProperty> cols(String... props) {
        List<OntologyProperty> cols = new ArrayList<>();
        for (String prop : props) {
            OntologyProperty property = Mockito.mock(OntologyProperty.class);
            Mockito.when(property.getName()).thenReturn(prop);
            cols.add(property);
        }
        return cols;
    }

    private static void formField(Map<String, IPropertyType> fields, String name, Object value) {
        PropertyType property = Mockito.mock(PropertyType.class);
        Mockito.when(property.getVal(Mockito.eq(false), Mockito.any())).thenReturn(value);
        fields.put(name, property);
    }

    private static PropertyMappingConfig mapping(String target, String parameterRef) {
        PropertyMappingConfig row = new PropertyMappingConfig();
        row.target = target;
        row.source = new FromParameterValueSource(parameterRef);
        return row;
    }
}
