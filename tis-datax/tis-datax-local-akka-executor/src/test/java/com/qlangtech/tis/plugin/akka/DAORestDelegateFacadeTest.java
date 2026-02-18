package com.qlangtech.tis.plugin.akka;

import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/2/7
 */
public class DAORestDelegateFacadeTest {

    @Test
    public void getWorkFlowBuildHistoryDAO() {
        DAORestDelegateFacade delegateFacade = DAORestDelegateFacade.createAKKAClusterDependenceDao();
        Assert.assertNotNull(delegateFacade);

        Assert.assertNotNull(delegateFacade.getWorkFlowBuildHistoryDAO());
        Assert.assertNotNull(delegateFacade.getDagNodeExecutionDAO());
    }

    @Test
    public void getDagNodeExecutionDAO() {
    }
}