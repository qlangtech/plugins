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

package com.qlangtech.tis.plugin.k8s;

import com.google.common.collect.Lists;
import com.qlangtech.tis.manage.common.Config;
import com.qlangtech.tis.pubhook.common.RunEnvironment;
import io.kubernetes.client.openapi.models.V1EnvVar;
import org.apache.commons.lang.StringUtils;

import java.util.List;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-06 16:32
 **/
public abstract class EnvVarsBuilder {
    private final String appName;

    public EnvVarsBuilder(String appName) {
        this.appName = appName;
    }

    public List<V1EnvVar> build() {
        List<V1EnvVar> envVars = Lists.newArrayList();
        V1EnvVar var = new V1EnvVar();
        var.setName("JVM_PROPERTY");
        var.setValue("-Ddata.dir=/opt/data -D" + Config.KEY_JAVA_RUNTIME_PROP_ENV_PROPS + "=true " + getExtraSysProps());
        envVars.add(var);

        RunEnvironment runtime = RunEnvironment.getSysRuntime();
        var = new V1EnvVar();
        var.setName("JAVA_RUNTIME");
        var.setValue(runtime.getKeyName());
        envVars.add(var);
        var = new V1EnvVar();
        var.setName("APP_OPTIONS");
        var.setValue(getAppOptions());
        envVars.add(var);

        var = new V1EnvVar();
        var.setName("APP_NAME");
        var.setValue(appName);
        envVars.add(var);

        var = new V1EnvVar();
        var.setName(Config.KEY_RUNTIME);
        var.setValue(runtime.getKeyName());
        envVars.add(var);

        var = new V1EnvVar();
        var.setName(Config.KEY_ZK_HOST);
        var.setValue(Config.getZKHost());
        envVars.add(var);

        var = new V1EnvVar();
        var.setName(Config.KEY_ASSEMBLE_HOST);
        var.setValue(Config.getAssembleHost());
        envVars.add(var);

        var = new V1EnvVar();
        var.setName(Config.KEY_TIS_HOST);
        var.setValue(Config.getTisHost());
        envVars.add(var);

        return envVars;

    }

//    public abstract String getAppOptions(String indexName, long timestamp) {
//        return indexName + " " + timestamp;
//    }

    public String getExtraSysProps() {
        return StringUtils.EMPTY;
    }

    public abstract String getAppOptions();
//   {
//        return indexName + " " + timestamp;
//    }
}
