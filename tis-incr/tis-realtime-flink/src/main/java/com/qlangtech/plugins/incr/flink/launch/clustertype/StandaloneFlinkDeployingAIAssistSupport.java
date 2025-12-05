package com.qlangtech.plugins.incr.flink.launch.clustertype;

import com.qlangtech.tis.config.BasicConfig;
import com.qlangtech.tis.extension.AIAssistSupport;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.model.UpdateCenterResource;
import com.qlangtech.tis.manage.common.ConfigFileContext;
import com.qlangtech.tis.manage.common.HttpUtils;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import org.apache.commons.lang3.SystemUtils;

import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.List;
import java.util.Map;

/**
 * TIS 初次安装 本地环境中还没有部署 Flink Standalone环境，如果用户还没有安装的话，需要Agent先将本地Standalone环境部署完毕
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2025/12/5
 */
public class StandaloneFlinkDeployingAIAssistSupport extends AIAssistSupport {
    @FormField(ordinal = 0, identity = true, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.identity})
    public String name;
    /**
     * flink 本地部署的目录
     */
    @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.absolute_path})
    public String flinkDeployDir;

    @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT, validate = {Validator.require, Validator.absolute_path})
    public String dataDir;

    @Override
    public boolean environmentSupport() {
        // 通过环境变量判断 , 确定当前是否为linux运行环境
        if (BasicConfig.inDockerContainer()) {
            return false;
        }
        return !SystemUtils.IS_OS_WINDOWS;
    }

    @Override
    public void startProcess() {
        // 整个部署执行过程
        // 1. 下载flink starndalone压缩包，2.解压，3. 修改java运行时变量 4. 启动运行

        // 确认flink 是否正常运行？如果正常运行中则退出，判断flink部署目录是否存在？8081端口是否打开？使用flink rest api判断 flink是否正常运行

        String flinkPkg = "flink-tis-1.20.1";
        URL flinkPkgTarUrl = UpdateCenterResource.getTISTarPkg(flinkPkg + "-bin.tar.gz");
        HttpUtils.get(flinkPkgTarUrl, new ConfigFileContext.StreamProcess<Void>() {
            @Override
            public Void p(int status, InputStream stream, Map<String, List<String>> headerFields) throws IOException {
                // 将下载包部署到 tmp目录中，解压tar包，将tar包部署到 flinkDeployDir属性指定的目录中
                return null;
            }
        });

        // 判断 dataDir 对应的目录是否存在 libs/plugins/ 子目录，目录中有多个*.tpi 文件，如果不满足则直接抛异常

        // 修改 flink-tis-1.20.1/conf/config.yaml 配置文件，修改点如下：
        // 1. 修改 jobmanager和taskmanager java运行环境变量： -Ddata.dir=this.dataDir, "-D"+CenterResource.KEY_notFetchFromCenterRepository=true


        // 启动flink
        // flink-tis-1.20.1/bin/start-cluster.sh
    }

    @Override
    public StandaloneFlinkDeployingAIAssistSupport createConfigInstance() {
        return this;
    }

    @Override
    public String identityValue() {
        return name;
    }

    @TISExtension
    public static class DefaultDesc extends BasicParamsConfigDescriptor {
        public DefaultDesc() {
            super("standalone-deploy-ai-assist");
        }

        @Override
        public String getDisplayName() {
            return "Deploy Local Flink";
        }
    }
}
