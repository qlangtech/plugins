package com.qlangtech.tis.plugin.datax.powerjob;

import com.qlangtech.tis.coredefine.module.action.impl.RcDeployment;
import com.qlangtech.tis.datax.job.PowerjobOrchestrateException;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.plugin.k8s.K8SController;
import com.qlangtech.tis.plugin.k8s.K8SUtils;
import io.kubernetes.client.openapi.ApiException;

/**
 * 使用power 默认的基于docker的内置的数据库
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/10/31
 */
public abstract class PowerjobCoreDataSource implements Describable<PowerjobCoreDataSource> {

    public abstract void launchMetaStoreService(K8SDataXPowerJobServer powerJobServer) throws ApiException, PowerjobOrchestrateException;

    public abstract String createCoreJdbcUrl();

    public abstract RcDeployment getRCDeployment(K8SController k8SController);
}
