package com.qlangtech.tis.plugin.datax.mongo;

import junit.framework.TestCase;
import org.junit.Assert;

import java.util.regex.Matcher;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/9/4
 */
public class TestMongoCMeta extends TestCase {
    public void testJsonPathPattern() {

        Matcher matcher = MongoCMeta.MongoDocSplitCMeta.PATTERN_JSON_PATH.matcher("abcd_ddd.kkkkk.bbbb");
        Assert.assertTrue(matcher.matches());

        matcher = MongoCMeta.MongoDocSplitCMeta.PATTERN_JSON_PATH.matcher("_abcd_ddd.kkkkk.bbbb");
        Assert.assertFalse(matcher.matches());

        matcher = MongoCMeta.MongoDocSplitCMeta.PATTERN_JSON_PATH.matcher("_abcd_ddd.kkkkk.bbbb[");
        Assert.assertFalse(matcher.matches());
    }
}
