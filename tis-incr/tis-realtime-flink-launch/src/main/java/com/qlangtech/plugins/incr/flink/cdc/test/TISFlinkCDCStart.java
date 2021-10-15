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

package com.qlangtech.plugins.incr.flink.cdc.test;

import com.qlangtech.tis.coredefine.module.action.IRCController;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.manage.common.CenterResource;
import com.qlangtech.tis.plugin.incr.IncrStreamFactory;
import com.qlangtech.tis.util.HeteroEnum;
import com.qlangtech.tis.util.IPluginContext;

import java.util.List;
import java.util.Objects;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-10-12 10:26
 **/
public class TISFlinkCDCStart {
    static final String dataxName = "mysql_elastic";

    public static void main(String[] args) throws Exception {

        //-classpath /Users/mozhenghua/j2ee_solution/project/plugins/tis-incr/tis-flink-dependency/target/tis-flink-dependency/WEB-INF/lib/*:/Users/mozhenghua/j2ee_solution/project/plugins/tis-incr/tis-flink-cdc-plugin/target/tis-flink-cdc-plugin/WEB-INF/lib/*:/Users/mozhenghua/j2ee_solution/project/plugins/tis-incr/tis-elasticsearch7-sink-plugin/target/tis-elasticsearch7-sink-plugin/WEB-INF/lib/*:/Users/mozhenghua/j2ee_solution/project/plugins/tis-incr/tis-realtime-flink/target/tis-realtime-flink/WEB-INF/lib/*:/Users/mozhenghua/j2ee_solution/project/plugins/tis-incr/tis-realtime-flink-launch/target/tis-realtime-flink-launch.jar:/Users/mozhenghua/j2ee_solution/project/plugins/tis-incr/tis-realtime-flink-launch/target/dependency/*:/Users/mozhenghua/j2ee_solution/project/plugins/tis-datax/tis-datax-elasticsearch-plugin/target/tis-datax-elasticsearch-plugin/WEB-INF/lib/*:

        CenterResource.setNotFetchFromCenterRepository();
        //Thread.currentThread().setContextClassLoader(TIS.get().pluginManager.uberClassLoader);

        IPluginContext pluginContext = IPluginContext.namedContext(dataxName);


        List<IncrStreamFactory> streamFactories = HeteroEnum.INCR_STREAM_CONFIG.getPlugins(pluginContext, null);
        IRCController incrController = null;
        for (IncrStreamFactory factory : streamFactories) {
            incrController = factory.getIncrSync();
        }
        Objects.requireNonNull(incrController, "stream app:" + dataxName + " incrController can not not be null");

        incrController.deploy(new TargetResName(dataxName), null, -1);
    }
}
