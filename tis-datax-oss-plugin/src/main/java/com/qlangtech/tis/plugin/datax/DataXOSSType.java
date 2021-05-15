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

package com.qlangtech.tis.plugin.datax;

import org.apache.commons.lang.StringUtils;

import java.util.Arrays;
import java.util.stream.Collectors;

/**
 * ref: https://github.com/alibaba/DataX/blob/master/ossreader/doc/ossreader.md#33-%E7%B1%BB%E5%9E%8B%E8%BD%AC%E6%8D%A2
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-13 15:02
 **/
public enum DataXOSSType {
    Long("long"),
    Double("double"),
    STRING("string"),
    Boolean("boolean"),
    Date("date");

    private final String literia;

    private DataXOSSType(String literia) {
        this.literia = literia;
    }

    public static DataXOSSType parse(String literia) {
        literia = StringUtils.lowerCase(literia);
        for (DataXOSSType t : DataXOSSType.values()) {
            if (literia.equals(t.literia)) {
                return t;
            }
        }
        return null;
    }

    public static String toDesc() {
        return Arrays.stream(DataXOSSType.values()).map((t) -> "'" + t.literia + "'").collect(Collectors.joining(","));
    }
}
