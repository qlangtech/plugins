package com.qlangtech.tis.plugin.datax.mongo;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.manage.common.TisUTF8;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.JDBCTypes;
import junit.framework.TestCase;
import org.bson.BsonType;
import org.bson.Document;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/9/3
 */
public class TestMongoColumnMetaData extends TestCase {
    @Test
    public void testParseMongoDocTypes() {

        Map<String, MongoColumnMetaData> colsSchema = Maps.newHashMap();
        List<Document> docs = IOUtils.loadResourceFromClasspath(TestMongoColumnMetaData.class //
                , "user-rows.json", true, (input) -> {
                    List<Document> result = Lists.newArrayList();

                    JSONObject obj = null;
                    JSONArray array = JSONArray.parseArray(org.apache.commons.io.IOUtils.toString(input, TisUTF8.get()));
                    for (Object o : array) {
                        obj = (JSONObject) o;
                        result.add(Document.parse(obj.toJSONString()));
                    }

                    return result;
                });
        Assert.assertEquals(3, docs.size());

        //   MongoColumnMetaData.parseMongoDocTypes( colsSchema, doc);
        for (Document doc : docs) {
            MongoColumnMetaData.parseMongoDocTypes(colsSchema, doc);
        }

        Assert.assertEquals(5, colsSchema.size());
        List<ColumnMetaData> metas = MongoColumnMetaData.reorder(colsSchema);
        metas.forEach((c) -> System.out.println(c.getName()));
        List<MongoColumnMetaData> assertCols = createAssertCols();
        MongoColumnMetaData assertCol = null;
        MongoColumnMetaData actualCol = null;

        for (int i = 0; i < assertCols.size(); i++) {
            assertCol = assertCols.get(i);
            actualCol = (MongoColumnMetaData) metas.get(i);
            Assert.assertEquals(actualCol.getName(), assertCol.getName(), actualCol.getName());
            Assert.assertEquals(actualCol.getName(), assertCol.getMongoFieldType(), actualCol.getMongoFieldType());
            Assert.assertEquals(actualCol.getName() + ",expect:" + assertCol.getType()
                    + ",actual:" + actualCol.getType(), assertCol.getType().getType(), actualCol.getType().getType());
        }


    }

    private static ArrayList<MongoColumnMetaData> createAssertCols() {

        ArrayList<MongoColumnMetaData> result = new ArrayList<>();
        // int index, String key, DataType dataType, BsonType mongoFieldType, int containValCount
        //            , boolean pk
        result.add(new MongoColumnMetaData(0, "array", DataType.getType(JDBCTypes.LONGVARCHAR), BsonType.ARRAY, 1, false));
        result.add(new MongoColumnMetaData(1, "profile", DataType.getType(JDBCTypes.LONGVARCHAR), BsonType.DOCUMENT, 1, false));
        result.add(new MongoColumnMetaData(2, "name", DataType.createVarChar(32), BsonType.STRING, 1, false));
        result.add(new MongoColumnMetaData(3, "_id", DataType.createVarChar(32), BsonType.STRING, 1, false));
        result.add(new MongoColumnMetaData(4, "age", DataType.getType(JDBCTypes.INTEGER), BsonType.INT32, 1, false));
        return result;
    }
}
