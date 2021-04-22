/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 *   This program is free software: you can use, redistribute, and/or modify
 *   it under the terms of the GNU Affero General Public License, version 3
 *   or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 *  This program is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *   FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package com.qlangtech.async.message.client.util;

/* *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2020/04/13
 */
public class Assert {

    public static void isNotNull(Object source, String... message) {
        if (source == null) {
            throw new RuntimeException(message[0]);
        }
    }

    public static void isTrue(boolean b, String message) {
        if (!b) {
            throw new RuntimeException(message);
        }
    }

    public static void isNotBlank(String source, String message) {
    // if (StringUtils.isBlank(source)) {
    // throw new RuntimeException(message);
    // }
    }
}
