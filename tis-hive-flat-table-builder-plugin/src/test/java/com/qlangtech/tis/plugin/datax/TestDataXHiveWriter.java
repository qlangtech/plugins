/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.tis.plugin.datax;

import com.alibaba.datax.common.element.ColumnCast;
import com.alibaba.datax.common.util.Configuration;
import com.alibaba.datax.core.job.JobContainer;
import com.alibaba.datax.core.util.container.JarLoader;
import com.alibaba.datax.core.util.container.LoadUtil;
import com.google.common.collect.Lists;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.ISelectedTab;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.extension.util.PluginExtraProps;
import com.qlangtech.tis.plugin.common.WriterTemplate;
import com.qlangtech.tis.plugin.test.BasicTest;
import com.qlangtech.tis.trigger.util.JsonUtil;
import com.qlangtech.tis.util.DescriptorsJSON;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-27 15:17
 **/
public class TestDataXHiveWriter extends BasicTest {

    public void testGetDftTemplate() {
        String dftTemplate = DataXHiveWriter.getDftTemplate();
        assertNotNull("dftTemplate can not be null", dftTemplate);
    }

    public void testPluginExtraPropsLoad() throws Exception {
        Optional<PluginExtraProps> extraProps = PluginExtraProps.load(DataXHiveWriter.class);
        assertTrue(extraProps.isPresent());
    }

    public void testDescriptorsJSONGenerate() {
        DataXHiveWriter writer = new DataXHiveWriter();
        DescriptorsJSON descJson = new DescriptorsJSON(writer.getDescriptor());
        System.out.println(JsonUtil.toString(descJson.getDescriptorsJSON()));
    }

    String mysql2hiveDataXName = "mysql2hive";

    public void testConfigGenerate() throws Exception {


        DataXHiveWriter hiveWriter = new DataXHiveWriter();
        hiveWriter.dataXName = mysql2hiveDataXName;
        hiveWriter.fsName = "hdfs1";
        hiveWriter.fileType = "text";
        hiveWriter.writeMode = "nonConflict";
        hiveWriter.fieldDelimiter = "\t";
        hiveWriter.compress = "gzip";
        hiveWriter.encoding = "utf-8";
        hiveWriter.template = DataXHiveWriter.getDftTemplate();
        hiveWriter.partitionRetainNum = 2;

        ISelectedTab.ColMeta colMeta = null;
        IDataxProcessor.TableMap tableMap = new IDataxProcessor.TableMap();
        tableMap.setTo("customer_order_relation");
        List<ISelectedTab.ColMeta> sourceCols = Lists.newArrayList();

        colMeta = new ISelectedTab.ColMeta();
        colMeta.setName("customerregister_id");
        colMeta.setType(ISelectedTab.DataXReaderColType.STRING);
        sourceCols.add(colMeta);

        colMeta = new ISelectedTab.ColMeta();
        colMeta.setName("waitingorder_id");
        colMeta.setType(ISelectedTab.DataXReaderColType.STRING);
        sourceCols.add(colMeta);

        colMeta = new ISelectedTab.ColMeta();
        colMeta.setName("kind");
        colMeta.setType(ISelectedTab.DataXReaderColType.INT);
        sourceCols.add(colMeta);

        colMeta = new ISelectedTab.ColMeta();
        colMeta.setName("create_time");
        colMeta.setType(ISelectedTab.DataXReaderColType.Long);
        sourceCols.add(colMeta);

        colMeta = new ISelectedTab.ColMeta();
        colMeta.setName("last_ver");
        colMeta.setType(ISelectedTab.DataXReaderColType.INT);
        sourceCols.add(colMeta);

        tableMap.setSourceCols(sourceCols);


        WriterTemplate.valiateCfgGenerate("hive-datax-writer-assert.json", hiveWriter, tableMap);


        hiveWriter.compress = null;
        hiveWriter.encoding = null;

        WriterTemplate.valiateCfgGenerate("hive-datax-writer-assert-without-option-val.json", hiveWriter, tableMap);

    }

    static final Field jarLoaderCenterField;

    static {
        try {
            jarLoaderCenterField = LoadUtil.class.getDeclaredField("jarLoaderCenter");
            jarLoaderCenterField.setAccessible(true);
        } catch (NoSuchFieldException e) {
            throw new RuntimeException("can not get field 'jarLoaderCenter' of LoadUtil", e);
        }
    }

    public void testDataDump() throws Exception {

        final DataxWriter dataxWriter = DataxWriter.load(null, mysql2hiveDataXName);

        realExecuteDump("hive-datax-writer-assert-without-option-val.json", dataxWriter);
    }


    protected void realExecuteDump(final String writerJson, DataxWriter dataxWriter) throws IllegalAccessException {
        Map<String, JarLoader> jarLoaderCenter = (Map<String, JarLoader>) jarLoaderCenterField.get(null);
        jarLoaderCenter.clear();

        final JarLoader uberClassLoader = new JarLoader(new String[]{"."});
        jarLoaderCenter.put("plugin.reader.streamreader", uberClassLoader);
        jarLoaderCenter.put("plugin.writer." + dataxWriter.getDataxMeta().getName(), uberClassLoader);

        Configuration allConf = IOUtils.loadResourceFromClasspath(MockDataxReaderContext.class //
                , "container.json", true, (input) -> {
                    Configuration cfg = Configuration.from(input);
                    cfg.set("plugin.writer." + dataxWriter.getDataxMeta().getName() + ".class"
                            , dataxWriter.getDataxMeta().getImplClass());
                    cfg.set("job.content[0].writer" //
                            , IOUtils.loadResourceFromClasspath(dataxWriter.getClass(), writerJson, true, (writerJsonInput) -> {
                                return Configuration.from(writerJsonInput);
                            }));

                    return cfg;
                });


        // 绑定column转换信息
        ColumnCast.bind(allConf);
        LoadUtil.bind(allConf);

        JobContainer container = new JobContainer(allConf);

        container.start();
    }

}
