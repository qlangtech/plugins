package com.qlangtech.tis.plugin.ontology.impl.action.rule;

import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.ontology.impl.action.ActionParameterConfig;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * {@code parameterRef} 下拉候选下发（{@code FromParameterValueSource.DftDescriptor}）防退化钉子。
 *
 * <p>候选装配是映射行 typo 的第一道防线：编辑态候选必须覆盖参数池全量参数
 * （val = parameterId，提交回送后映射行据此取值）；兜底通道绝不抛异常——
 * SELECTABLE 候选来源抛错会让整张表单打不开。
 */
public class TestFromParameterValueSourceOptions {

    /**
     * 参数池 → 候选：label 与 val 恒为 parameterId。刻意不用 displayName——
     * 映射表 / 参数池表格 / C1 标注 / C4 报错全说 parameterId，displayName 截断或重名反而难核对。
     */
    @Test
    public void poolParametersMapToParameterIdAsLabelAndVal() {
        ActionParameterConfig named = new ActionParameterConfig();
        named.parameterId = "chartxy_daily_report_report_id";
        named.displayName = "日报主键";

        ActionParameterConfig unnamed = new ActionParameterConfig();
        unnamed.parameterId = "chartxy_daily_report_detail_remark";
        unnamed.displayName = null;

        List<Option> options = FromParameterValueSource.DftDescriptor.poolToOptions(List.of(named, unnamed));

        Assert.assertEquals(2, options.size());
        Assert.assertEquals("label 恒为 parameterId，displayName 不参与（防截断 / 重名迷惑）",
                "chartxy_daily_report_report_id", options.get(0).getName());
        Assert.assertEquals("val 恒为 parameterId（提交回送的键）",
                "chartxy_daily_report_report_id", options.get(0).getValue());
        Assert.assertEquals("displayName 为空的参数同样直接用 parameterId",
                "chartxy_daily_report_detail_remark", options.get(1).getName());
    }

    /**
     * 创建向导早期参数池未落盘的 canonical 兜底通道：label 与 val 同为带前缀参数名。
     */
    @Test
    public void canonicalParamsMapToSelfNamedOptions() {
        List<Option> options = FromParameterValueSource.DftDescriptor.canonicalToOptions(
                List.of("chartxy_daily_report_report_id", "chartxy_daily_report_report_date"));

        Assert.assertEquals(2, options.size());
        Assert.assertEquals("chartxy_daily_report_report_id", options.get(0).getName());
        Assert.assertEquals("canonical 兜底通道 label=val", options.get(0).getName(), options.get(0).getValue());
    }

    /**
     * 候选加载在无 TIS 运行时的裸环境绝不抛——descriptor 渲染期对每个实例调用它，
     * 任何逃逸异常都会让整张表单打不开（与 {@code ModifyObjectRule} 同款兜底契约）。
     */
    @Test
    public void loadOptionsNeverThrowsOutsideRuntime() {
        FromParameterValueSource.DftDescriptor descriptor = new FromParameterValueSource.DftDescriptor();
        List<Option> options = descriptor.loadParameterRefOptions();
        Assert.assertNotNull("兜底路径必须返回列表而非 null", options);
    }
}
