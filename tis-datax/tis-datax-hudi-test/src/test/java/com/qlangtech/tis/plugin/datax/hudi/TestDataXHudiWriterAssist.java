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

package com.qlangtech.tis.plugin.datax.hudi;

import com.qlangtech.tis.extension.util.PluginExtraProps;
import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-05-13 12:03
 **/
public class TestDataXHudiWriterAssist {

    @Test
    public void testExtraProps() {
        Optional<PluginExtraProps> extraProps = PluginExtraProps.load(Optional.empty(), DataXHudiWriter.class);
        Assert.assertTrue("extraProps ", extraProps.isPresent());

        PluginExtraProps props = extraProps.get();
        PluginExtraProps.Props partitionedBy = props.get("partitionedBy");
        Assert.assertNotNull(partitionedBy);
        Assert.assertTrue("isAsynHelp", partitionedBy.isAsynHelp());
        Assert.assertTrue(partitionedBy.getAsynHelp(), StringUtils.length(partitionedBy.getAsynHelp()) > 1);
    }
}
