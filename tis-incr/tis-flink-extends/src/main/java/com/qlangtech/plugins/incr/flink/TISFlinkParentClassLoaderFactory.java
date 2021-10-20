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

package com.qlangtech.plugins.incr.flink;

import com.qlangtech.tis.TIS;
import org.apache.flink.util.FlinkUserCodeParentClassLoaderFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URL;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-10-16 17:51
 **/
public class TISFlinkParentClassLoaderFactory implements FlinkUserCodeParentClassLoaderFactory {
    private static final Logger logger = LoggerFactory.getLogger(TISFlinkParentClassLoaderFactory.class);

    @Override
    public ClassLoader createParent(URL[] urls) {
        logger.info("start to create ParentClassLoader");
        for (URL url : urls) {
            logger.info(url.toString());
        }
        return TIS.get().getPluginManager().uberClassLoader;
    }
}
