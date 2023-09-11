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

import com.google.common.collect.Lists;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IGroupChildTaskIterator;
import com.qlangtech.tis.datax.TableAlias;
import com.qlangtech.tis.extension.PluginFormProperties;
import com.qlangtech.tis.extension.impl.BaseSubFormProperties;
import com.qlangtech.tis.extension.impl.PropertyType;
import com.qlangtech.tis.extension.util.PluginExtraProps;
import com.qlangtech.tis.plugin.common.ReaderTemplate;
import com.qlangtech.tis.plugin.datax.mongo.MongoCMeta;
import com.qlangtech.tis.plugin.datax.mongo.MongoSelectedTabExtend;
import com.qlangtech.tis.plugin.datax.mongo.reader.ReaderFilteOff;
import com.qlangtech.tis.plugin.datax.test.TestSelectedTabs;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.JDBCTypes;
import com.qlangtech.tis.plugin.ds.mangodb.MangoDBDataSourceFactory;
import com.qlangtech.tis.plugin.ds.mangodb.TestMangoDBDataSourceFactory;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import com.qlangtech.tis.trigger.util.JsonUtil;
import com.qlangtech.tis.util.DescriptorsJSON;
import junit.framework.TestCase;
import org.apache.commons.collections.CollectionUtils;
import org.bson.BsonType;
import org.junit.Assert;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-08 11:35
 **/
public class TestDataXMongodbReader extends TestCase {

    private static final String DOC_TYPE_SPLIT_FIELD1 = "doc_type_split_field1";
    private static final String DOC_TYPE_SPLIT_FIELD2 = "doc_type_split_field2";

    private static final String DOC_TYPE_FIELD = "document_type_field";

    private List<String> assertCols = Lists.newArrayList("col1", "col2", "col3", DOC_TYPE_SPLIT_FIELD1,
            DOC_TYPE_SPLIT_FIELD2, DOC_TYPE_FIELD);

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
                IDataxProcessor.TableMap tabMapper = next.createTableMap(new TableAlias(next.mongoTable.getName()),
                        next.mongoTable);
                Assert.assertTrue(CollectionUtils.isEqualCollection(assertCols,
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

    private static List<SelectedTab> getSelectedTabs() {

        PluginFormProperties mongoReaderProps =
                TIS.get().getDescriptor(DataXMongodbReader.class).getSubPluginFormPropertyTypes("selectedTabs");
        PropertyType fieldProp = mongoReaderProps.accept(new PluginFormProperties.IVisitor() {

            @Override
            public PropertyType visit(BaseSubFormProperties props) {
                return props.getPropertyType("cols");
            }


        });

        Optional<CMeta.ElementCreatorFactory> metaCreator = fieldProp.extraProp.getCMetaCreator();
        Assert.assertTrue(metaCreator.isPresent());
        List<SelectedTab> tabs = TestSelectedTabs.createSelectedTabs(1, SelectedTab.class, metaCreator, (tab) -> {

            MongoCMeta addCol = (MongoCMeta) CMeta.create(metaCreator, DOC_TYPE_FIELD, JDBCTypes.LONGNVARCHAR);
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

            MongoSelectedTabExtend sourceExtend = new MongoSelectedTabExtend();
            sourceExtend.setName(tab.getName());
            sourceExtend.filter = new ReaderFilteOff();
            tab.setSourceProps(sourceExtend);
        });


        return tabs;
    }

    public void testPluginExtraPropsLoad() throws Exception {
        Optional<PluginExtraProps> extraProps = PluginExtraProps.load(DataXMongodbReader.class);
        assertTrue(extraProps.isPresent());
    }

    public void testDescriptorsJSONGenerate() {
        DataXMongodbReader reader = new DataXMongodbReader();
        DescriptorsJSON descJson = new DescriptorsJSON(reader.getDescriptor());

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
        List<ColumnMetaData> tableMetadata = reader.getTableMetadata(false, tab);
        Assert.assertTrue(tableMetadata.size() > 0);
    }

    private static DataXMongodbReader createMongodbReader() {
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
        reader.inspectRowCount = 200;
        reader.selectedTabs = getSelectedTabs();
        reader.template = DataXMongodbReader.getDftTemplate();
        return reader;
    }


}
