/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.plugins.incr.flink.connector.elasticsearch7;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.plugin.incr.TISSinkFactory;
import junit.framework.TestCase;

import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-09-29 16:55
 **/
public class TestElasticSearchSinkFactory extends TestCase {
    public void testLoadDescriptorLoad() {
        List<Descriptor<TISSinkFactory>> descriptors = TISSinkFactory.sinkFactory.descriptors();
        assertEquals(1, descriptors.size());

        assertEquals(ElasticSearchSinkFactory.DISPLAY_NAME_FLINK_CDC_SINK, descriptors.get(0).getDisplayName());
    }
}
