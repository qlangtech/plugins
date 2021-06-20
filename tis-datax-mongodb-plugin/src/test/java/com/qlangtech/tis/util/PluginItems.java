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

package com.qlangtech.tis.util;

import com.google.common.collect.Sets;
import com.qlangtech.tis.common.utils.Assert;
import com.qlangtech.tis.manage.common.Option;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-20 20:12
 **/
public class PluginItems {
    public static final String DB_NAME = "teatMongoDB";

    public static List<Option> getExistDbs(String... extendClass) {

        Set<String> extendClazzs = Sets.newHashSet("MongoDB");

        for (String sourceType : extendClass) {
            if (!extendClazzs.contains(sourceType)) {
                Assert.fail("param:" + sourceType + " must contain in:" + extendClazzs.stream().collect(Collectors.joining(",")));
            }
        }
//        String expectExtendClass = com.qlangtech.tis.plugin.ds.mysql.MySQLDataSourceFactory.class.getName();
//        if (!expectExtendClass.equals(extendClass)) {
//            Assert.fail("param:" + extendClass + " must equal with:" + expectExtendClass);
//        }

        return Collections.singletonList(new Option(DB_NAME, DB_NAME));

    }
}
