package com.qlangtech.tis.plugin.akka;

import com.qlangtech.tis.datax.DataXName;
import com.qlangtech.tis.exec.IExecChainContext;
import com.qlangtech.tis.workflow.dao.IDAGNodeExecutionDAO;
import com.qlangtech.tis.workflow.dao.IDatasourceDbDAO;
import com.qlangtech.tis.workflow.dao.IWorkFlowBuildHistoryDAO;
import com.qlangtech.tis.workflow.dao.IWorkFlowDAO;
import com.qlangtech.tis.workflow.dao.IWorkflowDAOFacade;
import com.qlangtech.tis.workflow.pojo.DagNodeExecution;
import com.qlangtech.tis.workflow.pojo.WorkFlowBuildHistory;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.Objects;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/2/4
 */
public class DAORestDelegateFacade implements IWorkflowDAOFacade {
    final IWorkFlowBuildHistoryDAO workflowBuildHistoryDAO;
    final IDAGNodeExecutionDAO dagNodeExecutionDAO;

    /**
     * @return
     * @see IDAGNodeExecutionDAO#insertSelective(DagNodeExecution)
     * @see IWorkFlowBuildHistoryDAO#updateByPrimaryKeySelective(WorkFlowBuildHistory)
     * @see IWorkFlowBuildHistoryDAO#loadFromWriteDB(Integer)
     */
    public static DAORestDelegateFacade createAKKAClusterDependenceDao() {

        Object dagNodeExecDAO = Proxy.newProxyInstance(DAORestDelegateFacade.class.getClassLoader()
                , new Class[]{IDAGNodeExecutionDAO.class} //
                , new InvocationHandler() {
                    @Override
                    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                        String methodName = method.getName();
                        switch (methodName) {
                            case "insertSelective": {
                                return IExecChainContext.insertDAGNodeExecution( //
                                        (DagNodeExecution) Objects.requireNonNull(args[0], "exec node can not be null"));
                            }
                            default:
                                throw new IllegalStateException("method:" + methodName + " is not supported for:" + IDAGNodeExecutionDAO.class.getSimpleName());
                        }
                    }
                });

        Object workflowBuildHistoryDAO = Proxy.newProxyInstance(DAORestDelegateFacade.class.getClassLoader()
                , new Class[]{IWorkFlowBuildHistoryDAO.class} //
                , new InvocationHandler() {
                    @Override
                    public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
                        String methodName = method.getName();
                        switch (methodName) {
                            case "hasRunningInstance": {
                                DataXName dataXName = (DataXName) Objects.requireNonNull(args[0], "param can not be null");

                            }
                            case "updateByPrimaryKeySelective": {
                                return IExecChainContext.updateWorkFlowBuildHistory( //
                                        (WorkFlowBuildHistory) Objects.requireNonNull(args[0], " task history can not be null"));
                            }
//                            case "insertSelective": {
//                                WorkFlowBuildHistory triggerHistory = (WorkFlowBuildHistory) Objects.requireNonNull(args[0], " task history can not be null");
//                                IExecChainContext.createNewTask(chainContext);
//                            }
                            case "loadFromWriteDB": {
                                return IExecChainContext.loadWorkFlowBuildHistory((Integer) args[0]);
                            }
                            default:
                                throw new IllegalStateException("method:" + methodName + " is not supported for:" + IWorkFlowBuildHistoryDAO.class.getSimpleName());
                        }

                    }
                });


        return new DAORestDelegateFacade((IWorkFlowBuildHistoryDAO) workflowBuildHistoryDAO, (IDAGNodeExecutionDAO) dagNodeExecDAO);
    }

    public DAORestDelegateFacade(IWorkFlowBuildHistoryDAO workflowBuildHistoryDAO, IDAGNodeExecutionDAO dagNodeExecutionDAO) {
        this.workflowBuildHistoryDAO = workflowBuildHistoryDAO;
        this.dagNodeExecutionDAO = dagNodeExecutionDAO;
    }

    @Override
    public IWorkFlowDAO getWorkFlowDAO() {
        throw new UnsupportedOperationException();
    }

    @Override
    public IWorkFlowBuildHistoryDAO getWorkFlowBuildHistoryDAO() {
        return workflowBuildHistoryDAO;
    }

    @Override
    public IDatasourceDbDAO getDatasourceDbDAO() {
        throw new UnsupportedOperationException();
    }

    @Override
    public IDAGNodeExecutionDAO getDagNodeExecutionDAO() {
        return this.dagNodeExecutionDAO;
    }
}
