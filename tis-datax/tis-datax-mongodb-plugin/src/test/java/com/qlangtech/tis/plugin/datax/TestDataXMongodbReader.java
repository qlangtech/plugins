/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.qlangtech.tis.plugin.datax;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.fastjson.JSONArray;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IGroupChildTaskIterator;
import com.qlangtech.tis.extension.PluginFormProperties;
import com.qlangtech.tis.extension.impl.BaseSubFormProperties;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.extension.impl.PropertyType;
import com.qlangtech.tis.extension.util.MultiItemsViewType;
import com.qlangtech.tis.extension.util.PluginExtraProps;
import com.qlangtech.tis.plugin.ValidatorCommons;
import com.qlangtech.tis.plugin.common.ReaderTemplate;
import com.qlangtech.tis.plugin.datax.mongo.MongoCMeta;
import com.qlangtech.tis.plugin.datax.mongo.MongoCMetaCreatorFactory;
import com.qlangtech.tis.plugin.datax.mongo.MongoSelectedTabExtend;
import com.qlangtech.tis.plugin.datax.mongo.TestMongoCMetaCreatorFactory;
import com.qlangtech.tis.plugin.datax.mongo.reader.ReaderFilteOff;
import com.qlangtech.tis.plugin.datax.test.TestSelectedTabs;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.ElementCreatorFactory;
import com.qlangtech.tis.plugin.ds.JDBCTypes;
import com.qlangtech.tis.plugin.ds.mangodb.MangoDBDataSourceFactory;
import com.qlangtech.tis.plugin.ds.mangodb.TestMangoDBDataSourceFactory;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import com.qlangtech.tis.test.TISEasyMock;
import com.qlangtech.tis.trigger.util.JsonUtil;
import com.qlangtech.tis.util.DefaultDescriptorsJSON;
import com.qlangtech.tis.util.DescriptorsJSON;
import junit.framework.TestCase;
import org.apache.commons.collections.CollectionUtils;
import org.bson.BsonType;
import org.easymock.EasyMock;
import org.junit.Assert;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-08 11:35
 **/
public class TestDataXMongodbReader extends TestCase implements TISEasyMock {

    private static final String DOC_TYPE_SPLIT_FIELD1 = "doc_type_split_field1";
    private static final String DOC_TYPE_SPLIT_FIELD2 = "doc_type_split_field2";

    private static final String DOC_TYPE_FIELD = "document_type_field";

    private List<String> assertCols = Lists.newArrayList("col1", "col2", "col3", DOC_TYPE_SPLIT_FIELD1,
            DOC_TYPE_SPLIT_FIELD2, DOC_TYPE_FIELD);

    private List<String> assertColsWithoutDocTypeField //
            = Lists.newArrayList("col1", "col2", "col3", DOC_TYPE_SPLIT_FIELD1, DOC_TYPE_SPLIT_FIELD2);

    @Override
    protected void setUp() throws Exception {
        super.setUp();
        this.clearMocks();
    }

    public void testGetDftTemplate() {
        String dftTemplate = DataXMongodbReader.getDftTemplate();
        assertNotNull("dftTemplate can not be null", dftTemplate);
    }

    public void testGetSubTasks() {
        DataXMongodbReader mongodbReader = createMongodbReader();
        try (IGroupChildTaskIterator subTasks = mongodbReader.getSubTasks((tab) -> true)) {
            int subTaskCount = 0;
            while (subTasks.hasNext()) {
                subTaskCount++;
                MongoDBReaderContext next = (MongoDBReaderContext) subTasks.next();
                Assert.assertNotNull(next);
                Assert.assertEquals(assertCols.stream().map((c) -> "\"`" + c + "`\"").collect(Collectors.joining(",")), next.getColsQuotes());

                // 这个保证下游DataXWriter 的cols 呈现何reader这边一致
                IDataxProcessor.TableMap tabMapper = next.createTableMap(Optional.empty(),//new TableAlias(next.mongoTable.getName()),
                        next.mongoTable);
                Assert.assertTrue(CollectionUtils.isEqualCollection(assertCols,
                        tabMapper.getSourceCols().stream().map((c) -> c.getName()).collect(Collectors.toList())));
            }

            Assert.assertEquals(1, subTaskCount);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        mongodbReader = createMongodbReader(true);

        try (IGroupChildTaskIterator subTasks = mongodbReader.getSubTasks((tab) -> true)) {
            int subTaskCount = 0;
            while (subTasks.hasNext()) {
                subTaskCount++;
                MongoDBReaderContext next = (MongoDBReaderContext) subTasks.next();
                Assert.assertNotNull(next);
                Assert.assertEquals(assertColsWithoutDocTypeField.stream().map((c) -> "\"`" + c + "`\"").collect(Collectors.joining(",")), next.getColsQuotes());

                // 这个保证下游DataXWriter 的cols 呈现何reader这边一致
                IDataxProcessor.TableMap tabMapper = next.createTableMap( Optional.empty(), //new TableAlias(next.mongoTable.getName()),
                        next.mongoTable);
                Assert.assertTrue(CollectionUtils.isEqualCollection(assertColsWithoutDocTypeField,
                        tabMapper.getSourceCols().stream().map((c) -> c.getName()).collect(Collectors.toList())));
            }

            Assert.assertEquals(1, subTaskCount);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    public void testGetSelectedTabs() throws Exception {
        List<SelectedTab> tabs = this.getSelectedTabs();
        Assert.assertEquals(1, tabs.size());

        for (SelectedTab tab : tabs) {
            List<CMeta> cols = tab.getCols();
            Assert.assertEquals(4, cols.size());

            for (CMeta c : cols) {
                Assert.assertTrue(c instanceof MongoCMeta);
            }

            SelectedTabExtend sourceProps = tab.getSourceProps();
            Assert.assertNotNull("sourceProps can not be null", sourceProps);
            Assert.assertEquals(TestSelectedTabs.tabNameOrderDetail, sourceProps.tabName);
            Assert.assertTrue(sourceProps instanceof MongoSelectedTabExtend);

            MongoSelectedTabExtend tabExtend = (MongoSelectedTabExtend) sourceProps;

            Assert.assertEquals(SelectedTabExtend.ExtendType.BATCH_SOURCE, tabExtend.getExtendType());
            Assert.assertNotNull(tabExtend.filter);
            Assert.assertTrue(tabExtend.filter instanceof ReaderFilteOff);
            return;
        }
        Assert.fail("can not reach here");
    }


    public void testValidateSubForm() {
        DataXMongodbReader reader = new DataXMongodbReader();
        DataXMongodbReader.DefaultDescriptor descriptor = (DataXMongodbReader.DefaultDescriptor) reader.getDescriptor();
        List<SelectedTab> tabs = this.getSelectedTabs();
        Assert.assertEquals(1, tabs.size());

        IControlMsgHandler msgHandler = mock("msgHandler", IControlMsgHandler.class);

        Context context = mock("context", Context.class);
        EasyMock.expect(context.hasErrors()).andReturn(false);
        this.replay();
        SelectedTab selectedTab = tabs.get(0);
        Assert.assertTrue(descriptor.validateSubForm(msgHandler, context, selectedTab));

        selectedTab.getCols();

        this.verifyAll();
    }


    public void testParseColsHtmlPost() {

        ElementCreatorFactory mongoElementCreator = new MongoCMetaCreatorFactory();
        IControlMsgHandler msgHandler = mock("msgHandler", IControlMsgHandler.class);
        Context context = mock("context", Context.class);

        // 执行validateSubForm 时会用到
        msgHandler.addFieldError(context, TestMongoCMetaCreatorFactory.createTestJsonPathFieldKey(), ValidatorCommons.MSG_EMPTY_INPUT_ERROR);
        EasyMock.expect(context.hasErrors()).andReturn(true);

        JSONArray colsJson = JSONArray.parseArray(IOUtils.loadResourceFromClasspath(TestDataXMongodbReader.class
                , "mongo_reader_mulit_select_cols_with_docfield_split_metas_jsonpath_empty.json"));

        this.replay();
        CMeta.ParsePostMCols<CMeta> parsePostMCols
                = mongoElementCreator.parsePostMCols(null, msgHandler, context, MultiItemsViewType.keyColsMeta, colsJson);
        Assert.assertNotNull(parsePostMCols);
        Assert.assertFalse(parsePostMCols.validateFaild);
        Assert.assertEquals(5, parsePostMCols.writerCols.size());

        Set<String> cols = Sets.newHashSet("array", "profile", "_id", "name", "age");

        Assert.assertTrue("cols shall be:" + String.join(",", cols), CollectionUtils.isEqualCollection(cols
                , parsePostMCols.writerCols.stream().map((col) -> col.getName()).collect(Collectors.toSet())));


        DataXMongodbReader.DefaultDescriptor mongoReaderDescriptor = new DataXMongodbReader.DefaultDescriptor();

        SelectedTab tab = new SelectedTab();
        tab.cols = parsePostMCols.writerCols;

        Assert.assertFalse("validate shall be faild"
                , mongoReaderDescriptor.validateSubForm(msgHandler, context, tab));

        this.verifyAll();
    }


    private static List<SelectedTab> getSelectedTabs() {
        return getSelectedTabs(false);
    }

    /**
     * @param disableDocTypeField 将doctypefile 字段失效，但是两个 拆分子字段仍然要显示
     * @return
     */
    private static List<SelectedTab> getSelectedTabs(boolean disableDocTypeField) {

        PluginFormProperties mongoReaderProps =
                TIS.get().getDescriptor(DataXMongodbReader.class).getSubPluginFormPropertyTypes("selectedTabs");
        PropertyType fieldProp = mongoReaderProps.accept(new PluginFormProperties.IVisitor() {

            @Override
            public PropertyType visit(BaseSubFormProperties props) {
                return props.getPropertyType("cols");
            }


        });

        ElementCreatorFactory metaCreator = fieldProp.getCMetaCreator();
        //  Assert.assertTrue(metaCreator.isPresent());
        List<SelectedTab> tabs = TestSelectedTabs.createSelectedTabs(1, SelectedTab.class, Optional.of((ElementCreatorFactory<CMeta>) metaCreator), (tab) -> {

            MongoCMeta addCol = (MongoCMeta) CMeta.create(Optional.of(metaCreator), DOC_TYPE_FIELD, JDBCTypes.LONGNVARCHAR);
            addCol.setDisable(disableDocTypeField);
            addCol.setMongoFieldType(BsonType.DOCUMENT);
            List<MongoCMeta.MongoDocSplitCMeta> docFieldSplitMetas = Lists.newArrayList();
            MongoCMeta.MongoDocSplitCMeta splitMeta = null;

            splitMeta = new MongoCMeta.MongoDocSplitCMeta();
            splitMeta.setName(DOC_TYPE_SPLIT_FIELD1);
            splitMeta.setJsonPath("document_type_field.child_filed1");
            splitMeta.setType(DataType.createVarChar(100));
            docFieldSplitMetas.add(splitMeta);

            splitMeta = new MongoCMeta.MongoDocSplitCMeta();
            splitMeta.setName(DOC_TYPE_SPLIT_FIELD2);
            splitMeta.setJsonPath("document_type_field.child_filed2");
            splitMeta.setType(DataType.createVarChar(100));
            docFieldSplitMetas.add(splitMeta);

            addCol.setDocFieldSplitMetas(docFieldSplitMetas);

            tab.cols.add(addCol);

            setMongoSourceTabExtend(tab);
        });


        return tabs;
    }

    public static void setMongoSourceTabExtend(SelectedTab tab) {
        MongoSelectedTabExtend sourceExtend = new MongoSelectedTabExtend();
        sourceExtend.setName(tab.getName());
        sourceExtend.filter = new ReaderFilteOff();
        tab.setSourceProps(sourceExtend);
    }

    public void testPluginExtraPropsLoad() throws Exception {
        Optional<PluginExtraProps> extraProps = PluginExtraProps.load(DataXMongodbReader.class);
        assertTrue(extraProps.isPresent());
    }

    public void testDescriptorsJSONGenerate() {
        DataXMongodbReader reader = new DataXMongodbReader();
        DescriptorsJSON descJson = new DefaultDescriptorsJSON(reader.getDescriptor());

        JsonUtil.assertJSONEqual(DataXMongodbReader.class, "mongdodb-datax-reader-descriptor.json",
                descJson.getDescriptorsJSON(), (m, e, a) -> {
                    assertEquals(m, e, a);
                });

    }

    public void testTemplateGenerate() throws Exception {
        String dataXName = "testDataXName";
        //MangoDBDataSourceFactory dsFactory = TestMangoDBDataSourceFactory.createMangoDBDS();// getDataSourceFactory();


        DataXMongodbReader reader = createMongodbReader();

        ReaderTemplate.validateDataXReader("mongodb-datax-reader-assert.json", dataXName, reader);
    }


    public void testGetTableMetadata() throws Exception {
        DataXMongodbReader reader = createMongodbReader();
        EntityName tab = EntityName.parse("user");
        List<ColumnMetaData> tableMetadata = reader.getTableMetadata(false, null, tab);
        Assert.assertTrue(tableMetadata.size() > 0);
    }

    private static DataXMongodbReader createMongodbReader() {
        return createMongodbReader(false);
    }

    private static DataXMongodbReader createMongodbReader(boolean disableDocTypeField) {
        DataXMongodbReader reader = new DataXMongodbReader() {

            @Override
            public MangoDBDataSourceFactory getDataSourceFactory() {
                return TestMangoDBDataSourceFactory.createMangoDBDS();
            }

            @Override
            public Class<?> getOwnerClass() {
                return DataXMongodbReader.class;
            }
        };
        // reader.inspectRowCount = 200;
        reader.selectedTabs = getSelectedTabs(disableDocTypeField);
        reader.template = DataXMongodbReader.getDftTemplate();
        return reader;
    }


}
