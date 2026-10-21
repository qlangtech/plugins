package com.qlangtech.tis.plugin.ontology.impl.action;

import com.alibaba.citrus.turbine.impl.DefaultContext;
import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.impl.PropValRewrite;
import com.qlangtech.tis.plugin.ontology.Ontology;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.plugin.ontology.impl.action.widget.impl.InputTextWidget;
import com.qlangtech.tis.runtime.module.misc.FormVaildateType;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.util.AttrValMap;
import org.junit.Assert;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.Optional;

/**
 * 运行时扁平表单（{@code ActionFormPreview}）的执行链路回归测试。
 *
 * <p>生成类不在 Sezpoz 编译期索引里，{@code TIS.get().getDescriptor(impl)} 永远反查不到，
 * 因此执行期必须「由调用方显式传入 descriptor」（{@link AttrValMap#parseDescribableMap(Optional,
 * JSONObject, Descriptor, com.qlangtech.tis.extension.impl.PropValRewrite)}）。
 * 本用例把这条链路端到端钉住：生成源码可编译 → 显式 descriptor 解析 → 校验 → 实例化 → 取字段值。
 *
 * <p>同时覆盖生成源码里的两处易错点：
 * <ul>
 *   <li>domain 的 extra-props key 必须是 {@code OntologyDomain.NAME_ONTOLOGY_DOMAIN}
 *       （写成 {@code ONTOLOGY_DOMAIN} 会拼进 HeteroEnum 的 toString，且 {@code $1} 会被 GString 插值）；</li>
 *   <li>生成类必须覆写 {@code getDescriptor()}，否则执行链路上的
 *       {@code Objects.requireNonNull(plugin.getDescriptor())} 会拿到 null。</li>
 * </ul>
 */
public class TestActionFormDescriptorBuilder {

    @Test
    public void testExplicitDescriptorRoundTrip() throws Exception {
        OntologyPluginMeta meta = OntologyPluginMeta.create(Ontology.OntologyEnum.Action, "temp_exec_domain")
                .setPluginIdVal("temp_action");

        ActionParameterConfig param = new ActionParameterConfig();
        param.parameterId = "newPriority";
        param.displayName = "新优先级";
        param.order = 0;
        PrimitiveDataType dataType = new PrimitiveDataType();
        dataType.widgetType = new InputTextWidget();
        dataType.required = true;
        param.dataType = dataType;

        ActionParameters params = new ActionParameters();
        params.parameters = new ArrayList<>();
        params.parameters.add(param);

        Descriptor<?> formDesc = ActionFormDescriptorBuilder.buildDescriptor(meta, params);
        Assert.assertNotNull("descriptor must be built", formDesc);
        Assert.assertEquals(ActionFormDescriptorBuilder.class.getPackageName() + ".ActionFormPreview",
                formDesc.getId());

        JSONObject body = JSONObject.parseObject("""
                {"impl":"com.qlangtech.tis.plugin.ontology.impl.action.ActionFormPreview",
                 "vals":{"newPriority":{"_primaryVal":"P1"}}}
                """);

        AttrValMap attrVals = AttrValMap.parseDescribableMap(
                Optional.empty(), body, formDesc, PropValRewrite.dftRewrite());
        Assert.assertSame(formDesc, attrVals.descriptor);

        DefaultContext context = new DefaultContext();
        IControlMsgHandler msgHandler = Mockito.mock(IControlMsgHandler.class);
        Descriptor.PluginValidateResult result = attrVals.validateWithScope(
                msgHandler, context, 0, 0, FormVaildateType.STRICT);
        Assert.assertTrue("must be valid", result.isValid());

        Describable<?> instance = result.newInstance();
        Assert.assertNotNull(instance);
        Assert.assertEquals("com.qlangtech.tis.plugin.ontology.impl.action.ActionFormPreview",
                instance.getClass().getName());
        Assert.assertEquals("P1", instance.getClass().getField("newPriority").get(instance));
        Assert.assertNotNull("生成的类必须有自带 descriptor 兜底", instance.getDescriptor());
    }
}
