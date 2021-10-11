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

package com.qlangtech.plugins.incr.flink.cdc;

import com.qlangtech.tis.async.message.client.consumer.AsyncMsg;
import com.qlangtech.tis.realtime.transfer.DTO;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
//import org.apache.flink.types.Row;
import org.glassfish.jersey.internal.guava.Sets;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-09-27 15:47
 **/
public class SourceChannel implements AsyncMsg<List<SourceFunction<DTO>>> {

    private final List<SourceFunction<DTO>> sourceFunction;
    private final Set<String> focusTabs = Sets.newHashSet();

    public SourceChannel(List<SourceFunction<DTO>> sourceFunction) {
        this.sourceFunction = sourceFunction;
    }

    @Override
    public List<SourceFunction<DTO>> getSource() throws IOException {
        return this.sourceFunction;
    }


    @Override
    public String getMsgID() {
        return null;
    }

    @Override
    public Set<String> getFocusTabs() {
        return this.focusTabs;
    }

    public void addFocusTab(String tab) {
        if (StringUtils.isEmpty(tab)) {
            throw new IllegalArgumentException("param tab can not be null");
        }
        this.focusTabs.add(tab);
    }


    @Override
    public String getTopic() {
        return null;
    }

    @Override
    public String getTag() {
        return null;
    }
}
