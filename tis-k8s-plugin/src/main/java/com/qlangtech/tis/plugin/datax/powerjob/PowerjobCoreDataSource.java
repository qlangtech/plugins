package com.qlangtech.tis.plugin.datax.powerjob;

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.coredefine.module.action.impl.RcDeployment;
import com.qlangtech.tis.datax.TimeFormat;
import com.qlangtech.tis.datax.job.IRegisterApp;
import com.qlangtech.tis.datax.job.PowerjobOrchestrateException;
import com.qlangtech.tis.datax.job.SSERunnable;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.ExtensionList;
import com.qlangtech.tis.plugin.k8s.K8SController;
import com.qlangtech.tis.plugin.k8s.NamespacedEventCallCriteria;
import io.kubernetes.client.openapi.ApiException;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.MessageFormat;

/**
 * 使用power 默认的基于docker的内置的数据库
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/10/31
 */
public abstract class PowerjobCoreDataSource implements Describable<PowerjobCoreDataSource> {

    protected static final MessageFormat KEY_USERNAME_AND_PASSWORD //
            = new MessageFormat("--spring.datasource.core.username={0} --spring.datasource.core.password={1}");//=root
    private static final Logger logger = LoggerFactory.getLogger(PowerjobCoreDataSource.class);

    private transient IRegisterApp registerApp;

    private IRegisterApp getTISPowerjobClient() {

        if (this.registerApp == null) {
            ExtensionList<IRegisterApp> extensionList = TIS.get().getExtensionList(IRegisterApp.class);
            for (IRegisterApp resisterApp : extensionList) {
                return this.registerApp = resisterApp;
            }

            throw new IllegalStateException("can not find instanceof " + IRegisterApp.class.getSimpleName());
        }

        return this.registerApp;
    }


    public final void initialPowerjobAccount(K8SDataXPowerJobServer powerJobServer) throws PowerjobOrchestrateException {
        SSERunnable sse = SSERunnable.getLocal();
        // powerJobServer.getK8SApi()
        // String powerjobDomain, String appName, String password
//        final String linkHost = powerJobServer.serverPortExport
//                .getPowerjobClusterHost(powerJobServer.getK8SApi(), powerJobServer.getImage().getNamespace());

        final String linkHost = powerJobServer.getPowerJobMasterGateway();

        IRegisterApp tisPowerJob = getTISPowerjobClient();
        (tisPowerJob) //
                .registerApp(linkHost, powerJobServer.appName, powerJobServer.password);

        // 需要打印日志
        logger.info("success initialPowerjobAccount");
        sse.info(K8SDataXPowerJobServer.K8S_DATAX_POWERJOB_REGISTER_ACCOUNT.getName(), TimeFormat.getCurrentTimeStamp()
                , "success initialPowerjobAccount with appName:" + powerJobServer.appName);
    }

    /**
     * 初始化注册Powerjob的账户
     *
     * @param
     */
    // @Override
    public final void waitServerLaunchedAndInitialPowerjobAccount(K8SDataXPowerJobServer powerjobServer) {
        //

//        try {
//            // 检查账户是否存在
//
//            // 存在则直接跳过
//
//            // 不存在？则创建
//            (tisPowerJob) //
//                    .registerApp(linkHost, powerjobServer.appName, powerjobServer.password);
//        } catch (Exception e) {
//            throw new IllegalStateException("server:" + linkHost + ",appname:" + powerjobServer.appName, e);
//        }
        this.startWaitServerLaunchedAndInitialPowerjobAccount(powerjobServer);
        int tryCount = 0;
        final int tryLimit = 20;
        while (true) {
            try {
                //this.serverPortExport.initialPowerjobAccount(this);
                // this.coreDS.initialPowerjobAccount(this);
                initialPowerjobAccount(powerjobServer);

                break;
            } catch (Exception e) {
                int idxOfError = -1;
                if (tryCount++ < tryLimit && ( //
                        (idxOfError = ExceptionUtils.indexOfThrowable(e, java.net.SocketTimeoutException.class)) > -1 ||
                                (idxOfError = ExceptionUtils.indexOfThrowable(e, java.net.ConnectException.class)) > -1)
                ) {

                    // 说明是超时等待片刻即可
                    logger.warn("contain " + ExceptionUtils.getThrowableList(e).get(idxOfError).getMessage()
                            + " tryCount:" + tryCount + ",powerjob GateWay:" + powerjobServer.getPowerJobMasterGateway() + " " + e.getMessage());

                    try {
                        Thread.sleep(4500);
                    } catch (InterruptedException ex) {
                        throw new RuntimeException(ex);
                    }
                } else {
                    // 其他日志直接中断了
                    throw new RuntimeException(e);
                }
            }
        }

    }

    protected void startWaitServerLaunchedAndInitialPowerjobAccount(K8SDataXPowerJobServer powerjobServer) {
    }


    // public abstract void initialPowerjobAccount(K8SDataXPowerJobServer powerJobServer) throws PowerjobOrchestrateException;

    /**
     * 发布对应的RC
     *
     * @param powerJobServer
     * @throws ApiException
     * @throws PowerjobOrchestrateException
     */
    public abstract NamespacedEventCallCriteria launchMetaStore(K8SDataXPowerJobServer powerJobServer) throws ApiException, PowerjobOrchestrateException;

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
