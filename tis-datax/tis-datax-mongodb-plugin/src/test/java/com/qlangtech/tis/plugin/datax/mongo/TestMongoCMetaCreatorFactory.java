package com.qlangtech.tis.plugin.datax.mongo;

import com.qlangtech.tis.plugin.datax.SelectedTab;
import org.junit.Assert;

import static com.qlangtech.tis.plugin.datax.mongo.MongoCMetaCreatorFactory.KEY_DOC_FIELD_SPLIT_METAS;

import org.junit.Test;
/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/10/16
 */
public class TestMongoCMetaCreatorFactory {
    public static String createTestJsonPathFieldKey() {
        return MongoCMetaCreatorFactory.splitMetasKey(1, 0, MongoCMetaCreatorFactory.KEY_JSON_PATH);
    }

    @Test
    public void testSplitMEtasKey() {
        Assert.assertEquals(SelectedTab.KEY_FIELD_COLS + "[1]." + KEY_DOC_FIELD_SPLIT_METAS + "[0].jsonPath", createTestJsonPathFieldKey());
    }
}
