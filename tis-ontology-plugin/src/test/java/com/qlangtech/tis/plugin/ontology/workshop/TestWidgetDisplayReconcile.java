package com.qlangtech.tis.plugin.ontology.workshop;

import com.qlangtech.tis.plugin.ontology.workshop.widget.AbsoluteSizingMode;
import com.qlangtech.tis.plugin.ontology.workshop.widget.AbstractWidgetDisplayConfig;
import com.qlangtech.tis.plugin.ontology.workshop.widget.AutoSizingMode;
import com.qlangtech.tis.plugin.ontology.workshop.widget.CompactDisplayConfig;
import com.qlangtech.tis.plugin.ontology.workshop.widget.WidgetDisplayConfig;
import org.junit.Assert;
import org.junit.Test;

/**
 * 「用哪个显示配置 impl 由 Widget 类型决定」的端到端映射，以及存量数据归一
 * （{@link WorkshopModule#reconcileDisplay(Class, AbstractWidgetDisplayConfig)}）。
 *
 * <p>为什么单开一个类：{@code TestWidgetVariableContract} 扫的是 sezpoz 索引里的真实
 * Widget 类（要 instantiate descriptor），这里只关心<b>目标类 → 归一结果</b>这条纯函数，
 * 不需要任何 Widget 实例，也就不受 TIS 运行期是否可用的影响。
 *
 * <p>归一逻辑存在的理由是「文件里的类名和 Widget 当前的类型可能对不上」：
 * {@code _display.xml} 的根元素决定读出来是哪个 impl，而 {@code configClazzOf} 只看 Widget
 * 类型 —— 两者不一致时既不能照单全收（精简类带着不存在的 height 语义），
 * 也不能整份丢掉（用户填的宽度是有效信息）。
 */
public class TestWidgetDisplayReconcile {

    private static final String WIDGET_ID = "88888888-8888-8888-8888-888888888888";

    /** 完整 → 精简：只保留 width，height / displayOptimization 丢弃 */
    @Test
    public void testFullConfigIsNarrowedToCompact() {
        WidgetDisplayConfig full = new WidgetDisplayConfig();
        full.widgetId = WIDGET_ID;
        full.width = absoluteWidth(400);
        full.height = new AutoSizingMode();
        full.displayOptimization = WidgetDisplayConfig.DisplayOptimization.neverUnmount;

        AbstractWidgetDisplayConfig reconciled =
                WorkshopModule.reconcileDisplay(CompactDisplayConfig.class, full);

        Assert.assertTrue("目标类是精简配置时应换成 CompactDisplayConfig，实际：" + reconciled,
                reconciled instanceof CompactDisplayConfig);
        Assert.assertSame("width 是两份 impl 共有的字段，必须原样带过去",
                full.width, ((CompactDisplayConfig) reconciled).width);
        Assert.assertEquals("身份必须跟着搬 —— 归一后的实例还要回答「我属于哪个 widget」",
                WIDGET_ID, reconciled.identityValue());
    }

    /** 精简 → 完整：width 带过去，height / displayOptimization 取目标类默认值 */
    @Test
    public void testCompactConfigIsWidenedToFull() {
        CompactDisplayConfig compact = new CompactDisplayConfig();
        compact.widgetId = WIDGET_ID;
        compact.width = absoluteWidth(200);

        AbstractWidgetDisplayConfig reconciled =
                WorkshopModule.reconcileDisplay(WidgetDisplayConfig.class, compact);

        Assert.assertTrue("目标类是完整配置时应换成 WidgetDisplayConfig，实际：" + reconciled,
                reconciled instanceof WidgetDisplayConfig);
        WidgetDisplayConfig full = (WidgetDisplayConfig) reconciled;
        Assert.assertSame("width 必须原样带过去", compact.width, full.width);
        Assert.assertEquals("身份必须跟着搬", WIDGET_ID, full.identityValue());
        Assert.assertNull("height 在精简配置里没有语义，应取默认值（null）", full.height);
        Assert.assertEquals("displayOptimization 应取目标类字段默认值",
                WidgetDisplayConfig.DisplayOptimization.dft, full.displayOptimization);
    }

    /** 类名一致是最常见的情况：原样返回同一个实例，不做任何重建 */
    @Test
    public void testMatchingImplIsReturnedAsIs() {
        WidgetDisplayConfig full = new WidgetDisplayConfig();
        full.width = absoluteWidth(320);

        Assert.assertSame("类名一致时不应重建实例（重建会丢掉无法经由字段搬运的状态）",
                full, WorkshopModule.reconcileDisplay(WidgetDisplayConfig.class, full));

        CompactDisplayConfig compact = new CompactDisplayConfig();
        Assert.assertSame(compact,
                WorkshopModule.reconcileDisplay(CompactDisplayConfig.class, compact));
    }

    /** 没配过 Display 的 widget：null 进 null 出，不能凭空造一份 */
    @Test
    public void testNullStaysNull() {
        Assert.assertNull(WorkshopModule.reconcileDisplay(WidgetDisplayConfig.class, null));
        Assert.assertNull(WorkshopModule.reconcileDisplay(CompactDisplayConfig.class, null));
    }

    private static AbsoluteSizingMode absoluteWidth(int pixels) {
        AbsoluteSizingMode width = new AbsoluteSizingMode();
        width.pixels = pixels;
        return width;
    }
}
