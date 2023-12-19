package com.qlangtech.tis.plugin.datax.powerjob;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.plugin.k8s.K8SUtils;
import io.kubernetes.client.openapi.ApiException;

/**
 * 使用power 默认的基于docker的内置的数据库
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/10/31
 */
public abstract class PowerjobCoreDataSource implements Describable<PowerjobCoreDataSource> {
    public static final K8SUtils.ServiceResName K8S_DATAX_POWERJOB_MYSQL_SERVICE = new K8SUtils.ServiceResName("powerjob-mysql");
    public static final K8SUtils.ServiceResName K8S_DATAX_POWERJOB_SERVER_SERVICE = new K8SUtils.ServiceResName("powerjob-server");
    public static final K8SUtils.ServiceResName K8S_DATAX_POWERJOB_SERVER_NODE_PORT_SERVICE = new K8SUtils.ServiceResName("powerjob-server-nodeport");

    public abstract void launchMetaStoreService(K8SDataXPowerJobServer powerJobServer) throws ApiException;

    public abstract String createCoreJdbcUrl();
}
