package com.qlangtech.tis.plugin.common;

import com.google.common.collect.Lists;
import com.qlangtech.tis.datax.DBDataXChildTask;
import com.qlangtech.tis.datax.IDataxGlobalCfg;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxReader;
import com.qlangtech.tis.datax.IDataxReaderContext;
import com.qlangtech.tis.datax.IDataxWriter;
import com.qlangtech.tis.datax.StoreResourceType;
import com.qlangtech.tis.datax.TableAliasMapper;
import com.qlangtech.tis.datax.impl.DataXCfgGenerator;
import com.qlangtech.tis.datax.impl.TransformerInfo;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.trigger.JobTrigger;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/9/17
 */
public abstract class BasicTemplate {
    public static DataXCfgGenerator createMockDataXCfgGenerator(String vmTplContent) {
        return createMockDataXCfgGenerator(vmTplContent, TableAliasMapper.Null);
    }

    public static DataXCfgGenerator createMockDataXCfgGenerator(String vmTplContent, TableAliasMapper tableAliasMapper) {
        if (StringUtils.isEmpty(vmTplContent)) {
            throw new IllegalArgumentException("param vmTplContent can not be empty");
        }
        DataXCfgGenerator dataProcessor = new DataXCfgGenerator(null, "testDataXName", new IDataxProcessor() {
            @Override
            public IDataxReader getReader(IPluginContext pluginCtx) {
                return null;
            }

            @Override
            public Pair<List<RecordTransformerRules>, IPluginStore>
            getRecordTransformerRulesAndPluginStore(IPluginContext pluginCtx, String tableName) {
                return Pair.of(Lists.newArrayList(), null);
            }

            @Override
            public IDataxReader getReader(IPluginContext pluginContext, ISelectedTab tab) {
                throw new UnsupportedOperationException();
            }

            @Override
            public Set<TransformerInfo> getTransformerInfo(IPluginContext pluginCtx, Map<String, List<DBDataXChildTask>> groupedChildTask) {
                return Set.of();
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
            public DataXCfgGenerator.GenerateCfgs getDataxCfgFileNames(IPluginContext pluginCtx, Optional<JobTrigger> partialTrigger) {
                return null;
            }

            @Override
            public TableAliasMapper getTabAlias(IPluginContext pluginCtx, boolean withDft) {
                return tableAliasMapper;
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
            protected String getTemplateContent(
                    IDataxReaderContext readerContext, IDataxReader reader
                    , IDataxWriter writer, Optional<RecordTransformerRules> transformerRules) {
                return vmTplContent;
            }
        };
        return dataProcessor;
    }
}
