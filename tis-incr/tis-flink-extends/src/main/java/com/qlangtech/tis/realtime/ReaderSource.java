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

package com.qlangtech.tis.realtime;

import com.qlangtech.tis.realtime.transfer.DTO;
import org.apache.commons.lang.StringUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-11-15 18:03
 **/
public class ReaderSource {
    public final SourceFunction<DTO> sourceFunc;
    public final String tokenName;

    public ReaderSource(String tokenName, SourceFunction<DTO> sourceFunc) {
        if (StringUtils.isEmpty(tokenName)) {
            throw new IllegalArgumentException("param tokenName can not be empty");
        }
        this.sourceFunc = sourceFunc;
        this.tokenName = tokenName;
    }
}
