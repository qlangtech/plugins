package com.qlangtech.tis.plugin.datax.powerjob;

import com.qlangtech.tis.coredefine.module.action.impl.RcDeployment;
import com.qlangtech.tis.datax.job.PowerjobOrchestrateException;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.plugin.k8s.K8SController;
import io.kubernetes.client.openapi.ApiException;
import org.apache.commons.lang.StringUtils;

import java.text.MessageFormat;

/**
 * 使用power 默认的基于docker的内置的数据库
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/10/31
 */
public abstract class PowerjobCoreDataSource implements Describable<PowerjobCoreDataSource> {

    protected static final MessageFormat KEY_USERNAME_AND_PASSWORD = new MessageFormat("--spring.datasource.core.username={0} --spring.datasource.core.password={1}");//=root


    /**
     * 初始化注册Powerjob的账户
     *
     * @param powerJobServer
     */
    public abstract void initialPowerjobAccount(K8SDataXPowerJobServer powerJobServer);

    /**
     * 发布对应的RC
     *
     * @param powerJobServer
     * @throws ApiException
     * @throws PowerjobOrchestrateException
     */
    public abstract void launchMetaStore(K8SDataXPowerJobServer powerJobServer) throws ApiException, PowerjobOrchestrateException;

    /**
     * 发布RC对应的Service
     *
     * @param powerJobServer
     * @throws ApiException
     */
    public abstract void launchMetaStoreService(K8SDataXPowerJobServer powerJobServer) throws ApiException;

    public final String createCoreJdbcParams() {
        return "--spring.datasource.core.jdbc-url=" + this.getJdbcUrl() + " " + StringUtils.trimToEmpty(this.getExtractJdbcParams());
    }

    protected abstract String getJdbcUrl();

    protected String getExtractJdbcParams() {
        return StringUtils.EMPTY;
    }

    public abstract RcDeployment getRCDeployment(K8SController k8SController);


}
