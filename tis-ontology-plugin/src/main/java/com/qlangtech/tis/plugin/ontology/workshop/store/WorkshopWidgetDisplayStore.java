package com.qlangtech.tis.plugin.ontology.workshop.store;

import com.qlangtech.tis.extension.impl.XmlFile;
import com.qlangtech.tis.plugin.ontology.workshop.widget.AbstractWidgetDisplayConfig;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Widget 显示配置的独立落盘存储（一个 widget 至多一份）。
 *
 * <pre>
 *   {TIS.pluginCfgRoot}/ontology/{domain}/workshop/{moduleName}/widgets/{encodedWidgetId}_display.xml
 * </pre>
 *
 * <p>与 {@link WorkshopWidgetStore} <b>共用 {@code widgets/} 目录</b>：显示配置和 widget
 * 是一对一关系，放同一个目录可以按 widget id 就近查找（{@code ls} 一眼看清谁配了 Display）。
 * 代价是两边都必须声明 {@link #acceptStorageKey(String)}，否则
 * {@link AbstractWorkshopEntityStore#listAll()} 会混进另一类实体、
 * {@link AbstractWorkshopEntityStore#deleteNotIn(java.util.Set)} 会把对方的文件删掉
 * （widget 的差集删除是最常走的一条路径）。
 *
 * <h3>身份：实例上的 {@code widgetId} + 存储键上的编码</h3>
 * 显示配置实体（{@link AbstractWidgetDisplayConfig}）持有 {@code widgetId}（identity，
 * 与所属 Widget 的 {@code id} 是同一个值）。文件名 = {@code encodeIdentity(widgetId)}
 * + {@link #DISPLAY_SUFFIX}（见 {@link #getStorageKey(String)}），读写两侧都能从实例拿到
 * 身份，不必再从文件名反解。
 * <p>
 * widget id 是前端生成的 UUID（{@link #encodeIdentity(String)} 编码后原样不变），
 * 但存量 widget 的 id 可能是含中文/空格的旧版名称，所以编码/解码是成对的。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/10/30
 */
public class WorkshopWidgetDisplayStore extends AbstractWorkshopEntityStore<AbstractWidgetDisplayConfig> {

    /** 显示配置文件的文件名后缀 —— 与 widget 自身的 {@code {id}.xml} 区分 */
    public static final String DISPLAY_SUFFIX = "_display";

    private static final WorkshopWidgetDisplayStoreGetter storeGetter = new WorkshopWidgetDisplayStoreGetter();

    private WorkshopWidgetDisplayStore(String ontologyDomainId, String moduleName) {
        super(ontologyDomainId, moduleName);
    }

    public static WorkshopWidgetDisplayStore create(String ontologyDomainId, String moduleName) {
        return new WorkshopWidgetDisplayStore(ontologyDomainId, moduleName);
    }

    @Override
    protected String getStoreDirName() {
        return WorkshopWidgetStore.WIDGETS_DIR_NAME;
    }

    @Override
    protected BasicWorkshopStoreGetter<AbstractWidgetDisplayConfig> getStoreGetter() {
        return storeGetter;
    }

    @Override
    protected String getStorageKey(String identity) {
        return encodeIdentity(identity) + DISPLAY_SUFFIX;
    }

    @Override
    protected boolean acceptStorageKey(String storageKey) {
        return storageKey.endsWith(DISPLAY_SUFFIX);
    }

    /**
     * 全部显示配置，按<b>所属 widget 的 id</b> 索引。没配过 Display 的 widget 不在 Map 里。
     *
     * <p>键从<b>文件名</b>解出，而不是读实例上的 {@code widgetId}：文件在谁的
     * {@code _display.xml} 上，它就是谁的配置 —— 实例字段若与文件名不一致（手工搬过文件、
     * 旧版数据），以文件名归一，避免这份配置挂到别的 widget 名下、真主反而读不到。
     *
     * <p>用 {@link LinkedHashMap} 保持文件名顺序，加载路径的输出顺序因此稳定。
     */
    public Map<String, AbstractWidgetDisplayConfig> listAllByWidgetId() {
        File storeDir = getStoreDir();
        if (!storeDir.isDirectory()) {
            return Collections.emptyMap();
        }
        Map<String, AbstractWidgetDisplayConfig> displays = new LinkedHashMap<>();
        for (File file : FileUtils.listFiles(storeDir, new String[]{XmlFile.KEY_XML_EXTENSION}, false)) {
            String storageKey = StringUtils.removeEnd(file.getName(), XmlFile.KEY_XML_DOT_EXTENSION);
            if (!acceptStorageKey(storageKey)) {
                continue;
            }
            String widgetId = decodeIdentity(StringUtils.removeEnd(storageKey, DISPLAY_SUFFIX));
            AbstractWidgetDisplayConfig display =
                    getStoreGetter().load(ontologyDomainId, Optional.of(moduleName), storageKey);
            if (display != null) {
                display.widgetId = widgetId;
                displays.put(widgetId, display);
            }
        }
        return displays;
    }

    /**
     * {@link AbstractWorkshopEntityStore#encodeIdentity(String)} 的逆变换，把文件名还原成 widget id。
     * <p>
     * 与编码一样用 URL 编解码（`+` 还原成空格），保证存量 widget 的中文 / 空格 / 括号 id 也能往返。
     */
    static String decodeIdentity(String encoded) {
        try {
            return URLDecoder.decode(encoded, StandardCharsets.UTF_8.name());
        } catch (UnsupportedEncodingException e) {
            throw new IllegalStateException(e);
        }
    }

    public static class WorkshopWidgetDisplayStoreGetter extends BasicWorkshopStoreGetter<AbstractWidgetDisplayConfig> {

        @Override
        protected Class<AbstractWidgetDisplayConfig> getPluginType() {
            return AbstractWidgetDisplayConfig.class;
        }

        @Override
        public File getAssistRootDir(String ontologyName, Optional<String> submodule) {
            return new File(super.getAssistRootDir(ontologyName, submodule), WorkshopWidgetStore.WIDGETS_DIR_NAME);
        }

        /**
         * 读回后补齐 {@code widgetId}。
         *
         * <p>2026-10-31 那一版把该字段删掉过，它写出的 {@code _display.xml} 里没有
         * {@code <widgetId>} 元素，XStream 反序列化出来是 {@code null}。
         * 身份本来就在文件名上，这里按文件名补回，否则那条数据会让
         * {@link AbstractWidgetDisplayConfig#identityValue()} 抛异常、
         * 整个模块的 {@code loadLayout} 跟着失败。
         *
         * <p>只补 null：新写出的文件里字段就是权威值（服务端写入时已经覆写成当前 widget 的 id）。
         *
         * @param pluginIdVal 存储键（{@code {encodedWidgetId}_display}），不是 widget id
         */
        @Override
        public AbstractWidgetDisplayConfig load(String ontologyDomain, Optional<String> submodule,
                                                String pluginIdVal) {
            AbstractWidgetDisplayConfig display = super.load(ontologyDomain, submodule, pluginIdVal);
            if (display != null && display.widgetId == null) {
                display.widgetId = WorkshopWidgetDisplayStore.decodeIdentity(
                        StringUtils.removeEnd(pluginIdVal, WorkshopWidgetDisplayStore.DISPLAY_SUFFIX));
            }
            return display;
        }
    }
}
