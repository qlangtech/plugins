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

package com.qlangtech.tis.hive;

import org.apache.commons.lang.StringUtils;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-28 12:38
 **/
public enum HdfsFileType {
    TEXTFILE("text"), ORC("orc");
    private final String type;

    public String getType() {
        return this.name();
    }

    private HdfsFileType(String format) {
        this.type = format;
    }

    public static HdfsFileType parse(String format) {
        for (HdfsFileType f : HdfsFileType.values()) {
            if (f.type.equals(StringUtils.lowerCase(format))) {
                return f;
            }
        }
        throw new IllegalStateException("format:" + format + " is illegal");
    }
}
