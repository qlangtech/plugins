package com.qlangtech.tis.plugin.ontology.workshop.model;

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.DescriptorExtensionList;
import com.qlangtech.tis.extension.impl.XmlFile;
import com.qlangtech.tis.plugin.ontology.workshop.WorkshopModule;
import com.qlangtech.tis.plugin.ontology.workshop.model.definition.VariableDefinitionConfig;
import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;

/**
 * {@link WorkshopVariable#deleteStored} 的落盘删除测试。
 *
 * <p>删除走「按存储约定定位文件 → 删文件 → 清插件缓存」的路径，本测试验证的正是
 * 定位约定：写入路径（{@code persistVariable} → KeyedPluginStore）以
 * {@link WorkshopVariable#normalizeName} 归一化后的名字落盘，删除路径必须命中同一个文件 ——
 * 用大小写不同的名字请求删除，断言磁盘上的小写文件确实被删掉。
 *
 * <p><b>运行前提</b>：{@code deleteStored} 收尾要经 {@code TIS.getPluginStore} 清进程内
 * 插件缓存，因此依赖 TIS 运行时；JDK17 下需要
 * {@code --add-opens java.base/java.lang=ALL-UNNAMED}。未就绪时整类跳过（与
 * {@code TestWorkshopVariableTypes} 口径一致）：
 * <pre>
 * mvn -o test -Dtest=TestWorkshopVariableDeleteStored \
 *     -DargLine="--add-opens java.base/java.lang=ALL-UNNAMED"
 * </pre>
 */
public class TestWorkshopVariableDeleteStored {

    /**
     * 足够独特的命名，避免碰真实环境里同名 domain / module
     */
    private static final String DOMAIN = "test-delete-stored-domain";
    private static final String MODULE = "test-module";
    private static final String VAR_NAME = "MyVar";

    private File variablesDir;

    @Before
    public void assumeTisReady() {
        boolean ready;
        try {
            TIS tis = TIS.get();
            // DescriptorExtensionList 是惰性的：真正的扩展扫描发生在 isEmpty() 上，
            // 必须在这里物化，否则扫描异常漏到 @Before 外会变成失败而非跳过
            DescriptorExtensionList<VariableDefinitionConfig, Descriptor<VariableDefinitionConfig>> list =
                    tis == null ? null : tis.getDescriptorList(VariableDefinitionConfig.class);
            ready = list != null && !list.isEmpty();
        } catch (Throwable t) {
            ready = false;
        }
        Assume.assumeTrue("当前环境未初始化 TIS（JDK17 下通常还缺 "
                + "--add-opens java.base/java.lang=ALL-UNNAMED），跳过本检查", ready);
        this.variablesDir = new File(
                WorkshopModule.getWorkshopDir(DOMAIN, MODULE), WorkshopVariable.VARIABLE_DIR_NAME);
    }

    @After
    public void cleanTestDir() {
        // 本测试在 TIS home 下造的整个 workshop 目录只包含自己写入的文件，整树删除
        FileUtils.deleteQuietly(WorkshopModule.getWorkshopDir(DOMAIN, MODULE));
    }

    @Test
    public void deleteHitsTheNormalizedFileRegardlessOfCase() throws Exception {
        FileUtils.forceMkdir(variablesDir);
        // 与写入路径（KeyedPluginStore.Key.getStoreFile）同一套文件名编码
        File variableFile = new File(variablesDir, Descriptor.getPluginFileName(
                WorkshopVariable.normalizeName(VAR_NAME)) + XmlFile.KEY_XML_DOT_EXTENSION);
        FileUtils.write(variableFile, "<stub/>", StandardCharsets.UTF_8);
        Assert.assertTrue(variableFile.isFile());

        // 请求名与落盘名大小写不同：删除前必须做同样的 normalizeName 才能命中
        Assert.assertTrue(WorkshopVariable.deleteStored(DOMAIN, MODULE, VAR_NAME.toUpperCase()));
        Assert.assertFalse("落盘文件应已被删除", variableFile.exists());

        // 再删一次：文件已不在，返回 false（变量不存在）
        Assert.assertFalse(WorkshopVariable.deleteStored(DOMAIN, MODULE, VAR_NAME));
    }
}
