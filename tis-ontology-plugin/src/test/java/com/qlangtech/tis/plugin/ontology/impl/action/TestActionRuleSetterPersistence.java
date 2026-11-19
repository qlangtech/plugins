package com.qlangtech.tis.plugin.ontology.impl.action;

import com.alibaba.citrus.turbine.impl.DefaultContext;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorExtensionList;
import com.qlangtech.tis.extension.OneStepOfMultiSteps;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.ontology.Ontology;
import com.qlangtech.tis.plugin.ontology.OntologyAction;
import com.qlangtech.tis.plugin.ontology.OntologyDomain;
import com.qlangtech.tis.plugin.ontology.impl.OntologyPluginMeta;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.CreateObjectRule;
import com.qlangtech.tis.plugin.ontology.impl.action.rule.ModifyObjectRule;
import com.qlangtech.tis.plugin.ontology.impl.action.type.ActionType;
import com.qlangtech.tis.plugin.ontology.impl.action.type.ObjectActionType;
import com.qlangtech.tis.util.IPluginContext;
import com.qlangtech.tis.util.PartialSettedPluginContext;
import com.qlangtech.tis.util.UploadPluginMeta;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

/**
 * {@link ActionRuleSetter#processPreSaved} 的<b>单独编辑 Rules 页签</b>分支回归测试
 * （{@code ArrayUtils.isEmpty(preSavedStepPlugins)} 为真的那条路径）。
 *
 * <h3>这条分支为什么必须有测试</h3>
 * 它在生产链路上只有一种触发方式：用户在 ActionType 详情页的 Rules 页签点保存 ——
 * 前端只提交 ActionRuleSetter 这一个步骤，向导的「前置步骤」数组为空。此时后端必须
 * <b>回读已落盘的整个 Action → 替换第二步 → 按同一 identity 覆盖写回</b>：
 * <ul>
 *   <li>读：{@link OntologyPluginMeta#createPluginMeta()} 从<b>线程上下文</b>取 meta，
 *       而 meta 里的 {@code pluginIdVal} 决定读哪个 XML。请求没铺上下文时这里是 NPE，
 *       不是「什么也没发生」。</li>
 *   <li>写：规则没有独立的存储目录，只能写回 {@code action-type/<name>.xml}。
 *       写错 store 的 pluginClass 会在 {@code setPlugins} 的
 *       {@code isAssignableFrom} 校验上抛；写错文件名则「保存成功」但下次读回来还是旧规则。</li>
 * </ul>
 * 两者都是「不报错就以为成功」的失败模式，所以这里一路断言到<b>磁盘上的 XML 再反序列化</b>。
 *
 * <h3>运行前提</h3>
 * 走的全是真实链路（线程上下文 → {@code TIS.getPluginStore} → XStream 落盘），因此依赖
 * TIS 运行期；JDK17 下需要 {@code --add-opens java.base/java.lang=ALL-UNNAMED}，
 * 未就绪时整类跳过（与 {@code TestWorkshopVariableDeleteStored} 口径一致）：
 * <pre>
 * mvn -o test -Dtest=TestActionRuleSetterPersistence \
 *     -DargLine="--add-opens java.base/java.lang=ALL-UNNAMED"
 * </pre>
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/11/19
 * @see ActionRuleSetter#processPreSaved(IPluginContext, com.alibaba.citrus.turbine.Context, OneStepOfMultiSteps[])
 */
public class TestActionRuleSetterPersistence {

    /** 足够独特，避免碰到真实环境里的同名 domain / action */
    private static final String ONTOLOGY_DOMAIN_ID = "test_action_rule_setter_domain";
    private static final String ACTION_NAME = "test_action_rule_setter";

    /** 落盘时已存在的旧规则名 —— 新增规则保存后它必须消失（整体替换而非追加） */
    private static final String OLD_RULE_NAME = "旧规则";
    /** 单独编辑 Rules 页签时提交的新规则名 */
    private static final String NEW_RULE_NAME = "新规则";

    private PartialSettedPluginContext pluginContext;

    @Before
    public void setUp() {
        boolean ready;
        try {
            TIS tis = TIS.get();
            // DescriptorExtensionList 是惰性的：真正的扩展扫描发生在 isEmpty() 上，
            // 必须在这里物化，否则扫描异常漏到 @Before 外会变成失败而非跳过
            DescriptorExtensionList<ActionType, Descriptor<ActionType>> list =
                    tis == null ? null : tis.getDescriptorList(ActionType.class);
            ready = list != null && !list.isEmpty();
        } catch (Throwable t) {
            ready = false;
        }
        Assume.assumeTrue("当前环境未初始化 TIS（JDK17 下通常还缺 "
                + "--add-opens java.base/java.lang=ALL-UNNAMED），跳过本检查", ready);

        // 生产路径（plugin_action 的 action 线程）本来就铺了这些：OntologyPluginMeta.createPluginMeta()
        // 从线程上下文的 UploadPluginMeta 里取 domain 与 pluginIdVal，没有降级分支
        this.pluginContext = new PartialSettedPluginContext();
        DefaultContext context = new DefaultContext();
        UploadPluginMeta.putPluginMeta(context, actionMeta().getDelegate());
        pluginContext.setContext(context);
        IPluginContext.setPluginContext(pluginContext);
    }

    @After
    public void cleanUp() {
        // thread-local 不清会污染同 JVM 的其他测试（surefire 默认复用 fork）
        IPluginContext.setPluginContext(null);
        FileUtils.deleteQuietly(OntologyDomain.getActionDir(ONTOLOGY_DOMAIN_ID));
    }

    // ===================================================================
    //  1. 空前置步骤 → 回读 Action、替换 Rules、整体写回
    // ===================================================================

    /**
     * Rules 页签单独保存：已落盘的 Action 里旧规则被换成新提交的规则，且<b>其它步骤不被破坏</b>。
     *
     * <p>断言走「磁盘 → 新反序列化实例」而不是内存里的那个对象：内存对象即使被改了，
     * 也可能是 {@code setRuleSetter} 改到了同一份引用，根本证明不了写回成功。
     */
    @Test
    public void testRuleSetterOverwritesPersistedAction() throws Exception {
        seedAction(ACTION_NAME, OLD_RULE_NAME);
        Assert.assertTrue("前置条件：Action 必须已经落盘", actionFile().exists());
        String beforeXml = FileUtils.readFileToString(actionFile(), "UTF-8");
        Assert.assertTrue("前置条件：旧规则应出现在落盘内容里", beforeXml.contains(OLD_RULE_NAME));

        ActionRuleSetter edited = ruleSetter(NEW_RULE_NAME, new CreateObjectRule());

        // 空数组 = 前端没带任何前置步骤，即「单独编辑 Rules 页签」
        edited.processPreSaved(pluginContext, new DefaultContext(), new OneStepOfMultiSteps[0]);

        // 1) 写回的仍是同一个文件（按 identity 覆盖，不产生第二个 xml）
        File actionDir = OntologyDomain.getActionDir(ONTOLOGY_DOMAIN_ID);
        Assert.assertNotNull(actionDir.list());
        Assert.assertEquals("Action 的 XML 必须原地覆盖，不能在 action-type 目录里多出文件",
                1, actionDir.listFiles((dir, name) -> name.endsWith(".xml")).length);

        // 2) 清掉进程内缓存后从磁盘读回 —— 文件没写成功这里会拿到旧规则
        DefaultOntologyAction reloaded = reloadFromDisk();
        Assert.assertEquals("新提交的规则应恰好一条", 1, reloaded.getActionTypes().size());
        ActionType rule = reloaded.getActionTypes().get(0);
        Assert.assertEquals("规则名应是本次提交的", NEW_RULE_NAME, rule.identityValue());
        Assert.assertTrue("规则类型应还原成 ObjectActionType，实际：" + rule,
                rule instanceof ObjectActionType);
        Assert.assertTrue("内层规则应还原成 CreateObjectRule，实际：" + rule.getRule(),
                rule.getRule() instanceof CreateObjectRule);

        // 3) 其它步骤必须原样保留：整体覆盖写回最怕的就是把 Metadata 等槽位写丢
        Assert.assertEquals("Metadata 第一步不能被写丢", ACTION_NAME, reloaded.getName());
        Assert.assertTrue("第二步槽位应是提交上来的 ActionRuleSetter，实际："
                        + reloaded.getMultiStepsSavedItems()[1],
                reloaded.getMultiStepsSavedItems()[1] instanceof ActionRuleSetter);
        // 步骤数保持存量形态（两步）：setRuleSetter 只在「下标越界」时才补长，替换已有槽位不扩数组
        Assert.assertEquals("替换已有槽位不应改变步骤数", 2, reloaded.getMultiStepsSavedItems().length);
    }

    /**
     * 同一条分支的另一种输入形态：{@code preSavedStepPlugins} 传 {@code null}。
     *
     * <p>{@code ArrayUtils.isEmpty} 对 null 与空数组都为真，两者走同一条分支；
     * 而 {@code parsePreviousSteps} 在「无前置步骤」时到底给 null 还是空数组取决于
     * 请求体形状，所以两种都要能落盘。
     */
    @Test
    public void testNullPreSavedStepsAlsoPersists() throws Exception {
        seedAction(ACTION_NAME, OLD_RULE_NAME);

        ActionRuleSetter edited = ruleSetter(NEW_RULE_NAME, new ModifyObjectRule());
        edited.processPreSaved(pluginContext, new DefaultContext(), null);

        DefaultOntologyAction reloaded = reloadFromDisk();
        Assert.assertEquals(NEW_RULE_NAME, reloaded.getActionTypes().get(0).identityValue());
        Assert.assertTrue("修改类规则应还原成 ModifyObjectRule",
                reloaded.getActionTypes().get(0).getRule() instanceof ModifyObjectRule);
    }

    // ===================================================================
    //  2. 有前置步骤 → 不落盘（走向导链路，由末步统一保存）
    // ===================================================================

    /**
     * 有前置步骤时<b>不得</b>落盘：这是向导内的第二步，整条 Action 要等末步一起保存。
     *
     * <p>若这里也写回，用户在第 2 步中途点「下一步」就会把半成品 Action 提前落盘，
     * 后续步骤失败时留下一个不完整的配置。
     */
    @Test
    public void testNoPersistenceWhenPreSavedStepsPresent() throws Exception {
        seedAction(ACTION_NAME, OLD_RULE_NAME);
        String beforeXml = FileUtils.readFileToString(actionFile(), "UTF-8");

        ActionRuleSetter edited = ruleSetter(NEW_RULE_NAME, new CreateObjectRule());
        edited.processPreSaved(pluginContext, new DefaultContext(),
                new OneStepOfMultiSteps[]{new ActionMetadata()});

        Assert.assertEquals("有前置步骤时应保持磁盘内容原样",
                beforeXml, FileUtils.readFileToString(actionFile(), "UTF-8"));

        DefaultOntologyAction reloaded = reloadFromDisk();
        Assert.assertEquals("规则不应被本次调用改写",
                OLD_RULE_NAME, reloaded.getActionTypes().get(0).identityValue());
    }

    // ===================================================================
    //  辅助
    // ===================================================================

    /**
     * 落盘一个两步（Metadata + Rules）的存量 Action —— 正是 Rules 页签单独编辑时磁盘上的形态。
     *
     * <p>刻意只给两步：真实存量配置就是这个形状，{@code readResolve}/{@code setRuleSetter}
     * 的补长逻辑全靠它才会被走到。
     */
    private void seedAction(String actionName, String ruleName) {
        StepSeededAction action = new StepSeededAction();
        ActionMetadata metadata = new ActionMetadata();
        metadata.name = actionName;
        metadata.displayName = "规则页签测试 Action";
        metadata.description = "Rules 页签单独保存的落盘链路回归";
        action.seedSteps(new OneStepOfMultiSteps[]{
                metadata,
                ruleSetter(ruleName, new CreateObjectRule())
        });

        IPluginStore<OntologyAction> store = actionStore(actionMeta());
        store.setPlugins(pluginContext, Optional.empty(),
                Collections.singletonList(new Descriptor.ParseDescribable<>(action)), false);
        // 刚 setPlugins 进去的是内存实例；不清缓存，下面的「前置条件」断言就证明不了真的写过盘
        store.cleanPlugins();
        Assert.assertTrue("seed 之后 XML 必须存在：" + actionFile().getAbsolutePath(),
                actionFile().exists());
    }

    /**
     * 从磁盘重新读回 Action。
     *
     * <p>必须先 {@code cleanPlugins()}：{@code getPlugin()} 命中进程内缓存时返回的是
     * 刚才 setPlugins 塞进去的<b>同一个对象</b>，文件写坏了也发现不了。
     */
    private DefaultOntologyAction reloadFromDisk() {
        IPluginStore<OntologyAction> store = actionStore(actionMeta());
        store.cleanPlugins();
        OntologyAction loaded = store.getPlugin();
        Assert.assertTrue("读回来的必须是 DefaultOntologyAction，实际：" + loaded,
                loaded instanceof DefaultOntologyAction);
        return (DefaultOntologyAction) loaded;
    }

    @SuppressWarnings("unchecked")
    private static IPluginStore<OntologyAction> actionStore(OntologyPluginMeta meta) {
        // 与 ActionRuleSetter#processPreSaved 同一处两段式强转：OntologyEnum.getPluginStore
        // 的返回类型被擦成了 IPluginStore<Ontology>
        return (IPluginStore<OntologyAction>) (IPluginStore<?>) Ontology.OntologyEnum.Action
                .getPluginStore(meta.setPersistence());
    }

    private static OntologyPluginMeta actionMeta() {
        return OntologyPluginMeta.create(Ontology.OntologyEnum.Action, ONTOLOGY_DOMAIN_ID)
                .setPluginIdVal(ACTION_NAME)
                .setPersistence();
    }

    private static File actionFile() {
        return new File(OntologyDomain.getActionDir(ONTOLOGY_DOMAIN_ID),
                Descriptor.getPluginFileName(ACTION_NAME));
    }

    private static ActionRuleSetter ruleSetter(String ruleName, com.qlangtech.tis.plugin.ontology.impl.action.rule.OntologyActionRule rule) {
        ObjectActionType actionType = new ObjectActionType();
        actionType.ruleName = ruleName;
        // 顺序约束按对象类型比较；单条规则不参与比较，这里给个稳定的值即可
        actionType.objectTypeReference = "Order";
        actionType.objectRule = rule;

        ActionRuleSetter setter = new ActionRuleSetter();
        // 必须是真正的 ArrayList：Arrays.asList 会被 XStream 按反射逐字段序列化，
        // JDK17 上直接 InaccessibleObjectException（见 TestActionRulesMulti 同一处注释）
        List<ActionType> rules = new ArrayList<>();
        rules.add(actionType);
        setter.actionTypes = rules;
        return setter;
    }

    /**
     * 只为在测试里写 {@code stepsPlugin}：该字段在 {@link OntologyAction} 里是 protected，
     * 其包路径（{@code com.qlangtech.tis.plugin.ontology}）与测试包不同，从外部写不到。
     * 用子类而不是反射，这样 seed 出来的对象类型仍然是真实的 {@link DefaultOntologyAction}。
     */
    private static final class StepSeededAction extends DefaultOntologyAction {
        void seedSteps(OneStepOfMultiSteps[] steps) {
            this.stepsPlugin = steps;
        }
    }
}