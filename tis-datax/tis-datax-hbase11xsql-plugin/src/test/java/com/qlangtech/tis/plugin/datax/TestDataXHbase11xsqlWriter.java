package com.qlangtech.tis.plugin.datax;

import com.qlangtech.tis.extension.util.PluginExtraProps;
import java.util.Optional;

import static org.junit.Assert.*;
import org.junit.Test;
/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-08 11:35
 **/
public class TestDataXHbase11xsqlWriter {
    @Test
    public void testGetDftTemplate() {
        String dftTemplate = DataXHbase11xsqlWriter.getDftTemplate();
        assertNotNull("dftTemplate can not be null", dftTemplate);
    }

    @Test
    public void testPluginExtraPropsLoad() throws Exception {
        Optional<PluginExtraProps> extraProps = PluginExtraProps.load(DataXHbase11xsqlWriter.class);
        assertTrue(extraProps.isPresent());
    }
}
