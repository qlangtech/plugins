package com.qlangtech.tis.plugin.common;

import com.qlangtech.tis.datax.IDataxGlobalCfg;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.datax.IDataxWriter;
import com.qlangtech.tis.datax.TableAliasMapper;
import com.qlangtech.tis.datax.impl.DataXCfgGenerator;
import com.qlangtech.tis.plugin.StoreResourceType;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.lang.StringUtils;

import java.io.File;
import java.io.IOException;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/9/17
 */
public abstract class BasicTemplate {
    public static DataXCfgGenerator createMockDataXCfgGenerator(String vmTplContent) {
        if (StringUtils.isEmpty(vmTplContent)) {
            throw new IllegalArgumentException("param vmTplContent can not be empty");
        }
        DataXCfgGenerator dataProcessor = new DataXCfgGenerator(null, "testDataXName", new IDataxProcessor() {
            @Override
            public IDataxReader getReader(IPluginContext pluginCtx) {
                return null;
            }

            @Override
            public IDataxWriter getWriter(IPluginContext pluginCtx, boolean validateNull) {
                return null;
            }

            @Override
            public File getDataxCfgDir(IPluginContext pluginCtx) {
                return null;
            }

            @Override
            public File getDataxCreateDDLDir(IPluginContext pluginContext) {
                return null;
            }

            @Override
            public void saveCreateTableDDL(IPluginContext pluginCtx, StringBuffer createDDL, String sqlFileName, boolean overWrite) throws IOException {

            }

            @Override
            public File getDataXWorkDir(IPluginContext pluginContext) {
                return null;
            }

            @Override
            public boolean isReaderUnStructed(IPluginContext pluginCtx) {
                return false;
            }

            @Override
            public boolean isRDBMS2UnStructed(IPluginContext pluginCtx) {
                return false;
            }

            @Override
            public boolean isRDBMS2RDBMS(IPluginContext pluginCtx) {
                return false;
            }

            @Override
            public boolean isWriterSupportMultiTableInReader(IPluginContext pluginCtx) {
                return false;
            }

            @Override
            public DataXCfgGenerator.GenerateCfgs getDataxCfgFileNames(IPluginContext pluginCtx) {
                return null;
            }

            @Override
            public TableAliasMapper getTabAlias(IPluginContext pluginCtx) {
                return null;
            }

            @Override
            public String identityValue() {
                return null;
            }

            @Override
            public StoreResourceType getResType() {
                return null;
            }

            @Override
            public IDataxGlobalCfg getDataXGlobalCfg() {
                return new IDataxGlobalCfg() {
                    @Override
                    public int getChannel() {
                        return 0;
                    }

                    @Override
                    public int getErrorLimitCount() {
                        return 0;
                    }

                    @Override
                    public float getErrorLimitPercentage() {
                        return 0;
                    }

                    @Override
                    public String getTemplate() {
                        return null;
                    }

                    @Override
                    public String identityValue() {
                        return null;
                    }
                };
            }
        }) {
            @Override
            protected String getTemplateContent(IDataxReader reader, IDataxWriter writer) {
                return vmTplContent;
            }
        };
        return dataProcessor;
    }
}
