package com.qlangtech.tis.plugin.ontology;

import org.junit.Assert;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/14
 */
public class EnableChatBITest {

    @Test
    public void load() {
        EnableChatBI enableChatBI = EnableChatBI.load("falcon_14");
        Assert.assertNull(enableChatBI);
    }
}