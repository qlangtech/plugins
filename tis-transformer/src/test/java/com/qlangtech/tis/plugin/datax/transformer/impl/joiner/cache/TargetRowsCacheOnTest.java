package com.qlangtech.tis.plugin.datax.transformer.impl.joiner.cache;

import com.qlangtech.tis.plugin.datax.transformer.impl.joiner.TargetRowsCache;
import org.junit.Assert;
import org.junit.Test;

import java.time.Duration;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/1/27
 */
public class TargetRowsCacheOnTest {

    @Test
    public void testGetFromCache() {
        TargetRowsCacheOn cacheOn = new TargetRowsCacheOn();
        cacheOn.expireAfterAccess = Duration.ofSeconds(120);
        cacheOn.expireAfterWrite = Duration.ofSeconds(120);
        cacheOn.maxSize = 10000L;
        TargetRowsCache.JoinCacheKey cacheKey = new TargetRowsCache.JoinCacheKey();
        cacheKey.addParam("order_id").addPrimaryVal("123456787");
        cacheKey.addParam("is_valid").addParam(1);
        TargetRowsCache.JoinCacheValue cacheValue = new TargetRowsCache.JoinCacheValue();
        cacheOn.set2Cache(cacheKey, cacheValue);

        TargetRowsCache.JoinCacheKey cacheKey2 = new TargetRowsCache.JoinCacheKey();
        cacheKey2.addParam("order_id").addPrimaryVal("123456787");
        cacheKey2.addParam("is_valid").addParam(1);

        Assert.assertNotNull("cacheKey2 relevant instance can not be null", cacheOn.getFromCache(cacheKey2));

        cacheKey2 = new TargetRowsCache.JoinCacheKey();
        cacheKey2.addParam("order_id").addPrimaryVal("99999999");
        cacheKey2.addParam("is_valid").addParam(1);
        Assert.assertNull("cacheKey2 relevant instance can not be null", cacheOn.getFromCache(cacheKey2));
    }
}