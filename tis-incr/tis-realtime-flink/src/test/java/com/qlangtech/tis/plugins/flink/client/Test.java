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

package com.qlangtech.tis.plugins.flink.client;

import java.net.URL;
import java.util.Enumeration;


public class Test {
    public static void main(String[] args) throws Exception {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();

        Enumeration<URL> resources = classLoader.getResources("org/apache/kafka/connect/json/JsonSerializer.class");
        URL u = null;

        while (resources.hasMoreElements()) {
            u = resources.nextElement();
            System.out.println(u.toString());
        }

        System.out.println("================================================");
        resources = classLoader.getResources("com/fasterxml/jackson/core/JsonGenerator.class");
        while (resources.hasMoreElements()) {
            u = resources.nextElement();
            System.out.println(u.toString());
        }

    }
}
