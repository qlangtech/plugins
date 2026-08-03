package com.qlangtech.tis.plugin.datax.transformer.impl.joiner.cache;

import com.qlangtech.tis.plugin.datax.transformer.impl.joiner.TargetRowsCache;
import org.junit.Assert;
import org.junit.Test;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author 百岁 (baisui@qlangtech.com)
 */
public class TargetRowsCacheFullTest {

    @Test
    public void testPreloadHitAndMiss() {
        TargetRowsCacheFull cache = new TargetRowsCacheFull();
        cache.maxRows = 100L;

        cache.preload((sink) -> {
            sink.accept(createKey("100"), createValue("v100"));
            sink.accept(createKey("200"), createValue("v200"));
            sink.accept(createKey("300"), createValue("v300"));
        });

        Assert.assertNotNull(cache.getFromCache(createKey("100")));
        Assert.assertEquals("v100", cache.getFromCache(createKey("100")).get("col"));
        Assert.assertNull(cache.getFromCache(createKey("999")));
    }

    @Test
    public void testDuplicateKeyFirstWin() {
        TargetRowsCacheFull cache = new TargetRowsCacheFull();
        cache.maxRows = 100L;

        cache.preload((sink) -> {
            sink.accept(createKey("dup"), createValue("first"));
            sink.accept(createKey("dup"), createValue("second"));
        });

        Assert.assertEquals("first", cache.getFromCache(createKey("dup")).get("col"));
    }

    @Test(expected = IllegalStateException.class)
    public void testMaxRowsExceeded() {
        TargetRowsCacheFull cache = new TargetRowsCacheFull();
        cache.maxRows = 2L;

        cache.preload((sink) -> {
            sink.accept(createKey("1"), createValue("v1"));
            sink.accept(createKey("2"), createValue("v2"));
            sink.accept(createKey("3"), createValue("v3"));
        });
    }

    @Test(expected = IllegalStateException.class)
    public void testGetFromCacheBeforePreload() {
        TargetRowsCacheFull cache = new TargetRowsCacheFull();
        cache.maxRows = 100L;
        cache.getFromCache(createKey("100"));
    }

    @Test
    public void testAfterSavedReset() {
        TargetRowsCacheFull cache = new TargetRowsCacheFull();
        cache.maxRows = 100L;

        cache.preload((sink) -> sink.accept(createKey("100"), createValue("v100")));
        Assert.assertNotNull(cache.getFromCache(createKey("100")));

        cache.afterSaved(null, java.util.Optional.empty());

        cache.preload((sink) -> sink.accept(createKey("200"), createValue("v200")));
        Assert.assertNotNull(cache.getFromCache(createKey("200")));
        Assert.assertNull(cache.getFromCache(createKey("100")));
    }

    @Test
    public void testGetUDFDesc() {
        TargetRowsCacheFull cache = new TargetRowsCacheFull();
        cache.maxRows = 123L;
        Assert.assertEquals(1, cache.getUDFDesc().size());
        Assert.assertEquals("maxRows", cache.getUDFDesc().get(0).getPairs().get(0).getName());
        Assert.assertEquals("123", cache.getUDFDesc().get(0).getPairs().get(0).getValue());
    }

    @Test
    public void testPreloadOnlyOnce() {
        TargetRowsCacheFull cache = new TargetRowsCacheFull();
        cache.maxRows = 100L;
        AtomicInteger count = new AtomicInteger(0);
        cache.preload((sink) -> {
            count.incrementAndGet();
            sink.accept(createKey("100"), createValue("v100"));
        });
        cache.preload((sink) -> {
            count.incrementAndGet();
            sink.accept(createKey("200"), createValue("v200"));
        });
        Assert.assertEquals(1, count.get());
        Assert.assertNotNull(cache.getFromCache(createKey("100")));
        Assert.assertNull(cache.getFromCache(createKey("200")));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testSet2CacheNotSupported() {
        TargetRowsCacheFull cache = new TargetRowsCacheFull();
        cache.maxRows = 100L;
        cache.set2Cache(createKey("100"), createValue("v100"));
    }

    private static TargetRowsCache.JoinCacheKey createKey(String primaryVal) {
        TargetRowsCache.JoinCacheKey key = new TargetRowsCache.JoinCacheKey();
        key.addParam("id").addPrimaryVal(primaryVal);
        return key;
    }

    private static TargetRowsCache.JoinCacheValue createValue(String val) {
        TargetRowsCache.JoinCacheValue value = new TargetRowsCache.JoinCacheValue();
        value.setNull(false);
        value.put("col", val);
        return value;
    }
}
