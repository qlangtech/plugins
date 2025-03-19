package com.qlangtech.tis.datax.join;

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.assemble.FullbuildPhase;
import com.qlangtech.tis.cloud.ITISCoordinator;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.IDataxWriter;
import com.qlangtech.tis.datax.RpcUtils;
import com.qlangtech.tis.exec.AbstractExecContext;
import com.qlangtech.tis.exec.ExecutePhaseRange;
import com.qlangtech.tis.exec.ExecuteResult;
import com.qlangtech.tis.exec.IExecChainContext;
import com.qlangtech.tis.fullbuild.IFullBuildContext;
import com.qlangtech.tis.fullbuild.phasestatus.IJoinTaskStatus;
import com.qlangtech.tis.fullbuild.phasestatus.impl.JoinPhaseStatus;
import com.qlangtech.tis.fullbuild.taskflow.DataflowTask;
import com.qlangtech.tis.fullbuild.taskflow.IFlatTableBuilder;
import com.qlangtech.tis.job.common.JobCommon;
import com.qlangtech.tis.job.common.JobParams;
import com.qlangtech.tis.offline.DataxUtils;
import com.qlangtech.tis.plugin.StoreResourceType;
import com.qlangtech.tis.plugin.ds.IDataSourceFactoryGetter;
import com.qlangtech.tis.powerjob.TriggersConfig;
import com.qlangtech.tis.sql.parser.ISqlTask;
import com.qlangtech.tis.sql.parser.SqlTaskNodeMeta;
import com.qlangtech.tis.sql.parser.er.IPrimaryTabFinder;
import com.qlangtech.tis.sql.parser.tuple.creator.EntityName;
import com.tis.hadoop.rpc.RpcServiceReference;
import com.tis.hadoop.rpc.StatusRpcClientFactory;
import com.tis.hadoop.rpc.StatusRpcClientFactory.AssembleSvcCompsite;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Properties;
import java.util.function.Consumer;
import java.util.function.Supplier;

import static com.qlangtech.tis.sql.parser.ISqlTask.KEY_EXECUTE_TYPE;
import static com.qlangtech.tis.sql.parser.ISqlTask.KEY_EXPORT_NAME;
import static com.qlangtech.tis.sql.parser.ISqlTask.KEY_ID;
import static com.qlangtech.tis.sql.parser.ISqlTask.KEY_SQL_SCRIPT;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/12/7
 */
public class DataXJoinProcessExecutor {
    private static final Logger logger = LoggerFactory.getLogger(DataXJoinProcessExecutor.class);

    private static Options getOpts() {
        // create Options object
        Options options = new Options();

        //        OptionGroup group = new OptionGroup();
        //        group.setRequired(true);
        //        group.setSelected();
        //        options.addOptionGroup(group);

        //        JSONObject sqlTaskCfg = new JSONObject();
        //        sqlTaskCfg.put(KEY_ID, args[0]);
        //        sqlTaskCfg.put(KEY_SQL_SCRIPT, args[1]);
        //        sqlTaskCfg.put(KEY_EXECUTE_TYPE, args[2]);
        //        sqlTaskCfg.put(KEY_EXPORT_NAME, args[3]);

        addOpt(options, KEY_ID);
        addOpt(options, KEY_SQL_SCRIPT);
        addOpt(options, KEY_EXECUTE_TYPE);
        addOpt(options, KEY_EXPORT_NAME);

        addOpt(options, JobParams.KEY_TASK_ID);
        addOpt(options, IFullBuildContext.DRY_RUN, (builder) -> {
            builder.hasArg(false);
            builder.required(false);
        });
        addOpt(options, JobParams.KEY_COLLECTION);
        addOpt(options, DataxUtils.EXEC_TIMESTAMP);

        addOpt(options, DataXJoinProcessConsumer.Command.KEY_TABLE_GROUP, (builder) -> {
            builder.valueSeparator().numberOfArgs(2);
        });
        return options;
    }

    private static void addOpt(Options options, String key, Consumer<Option.Builder>... append) {
        Option.Builder builder = Option.builder();
        builder.option(key).required().hasArg();

        for (Consumer<Option.Builder> c : append) {
            c.accept(builder);
        }
        options.addOption(builder.build());
    }

    /**
     * @param args
     * @throws Exception
     * @see DataXJoinProcessConsumer
     */
    public static void main(String[] args) throws Exception {
        // create the parser
        CommandLineParser parser = new DefaultParser();
        CommandLine line = null;
        try {
            // parse the command line arguments
            line = parser.parse(getOpts(), args);
        } catch (ParseException exp) {
            throw new RuntimeException("Parsing failed.  Reason: " + exp.getMessage() + "args:" + String.join(",",
                    args), exp);
        }


        RpcServiceReference statusRpc = StatusRpcClientFactory.getService(ITISCoordinator.create());
        AssembleSvcCompsite.statusRpc = (statusRpc);

        try {
            AbstractExecContext execContext = createDftExecContent(line);
            JobCommon.setMDC(execContext.getTaskId(), null);

            //        cmdLine.addArgument(sqlTskJson.getString(KEY_ID));
            //        cmdLine.addArgument(sqlTskJson.getString(KEY_SQL_SCRIPT));
            //        cmdLine.addArgument(sqlTskJson.getString(KEY_EXECUTE_TYPE));
            //        cmdLine.addArgument(sqlTskJson.getString(KEY_EXPORT_NAME));

            JSONObject sqlTaskCfg = new JSONObject();
            sqlTaskCfg.put(KEY_ID, line.getOptionValue(KEY_ID));
            sqlTaskCfg.put(KEY_SQL_SCRIPT, line.getOptionValue(KEY_SQL_SCRIPT));
            sqlTaskCfg.put(KEY_EXECUTE_TYPE, line.getOptionValue(KEY_EXECUTE_TYPE));
            sqlTaskCfg.put(KEY_EXPORT_NAME, line.getOptionValue(KEY_EXPORT_NAME));
            ISqlTask.SqlTaskCfg sqlCfg = ISqlTask.toCfg(sqlTaskCfg);
            logger.info("start join process:{},sqlScript:{}", sqlCfg.getExportName(), sqlCfg.getSqlScript());
            SqlTaskNodeMeta sqlTask = SqlTaskNodeMeta.deserializeTaskNode(sqlCfg);

            executeJoin(statusRpc, execContext, sqlTask);
            logger.info("exit the process:{},sqlScript:{}", sqlCfg.getExportName(), sqlCfg.getSqlScript());
            System.exit(0);
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }

    public static JSONObject deserializeInstanceParams(CommandLine line) {

        JSONObject instanceParams = new JSONObject();

        //        Integer taskId = Objects.requireNonNull(instanceParams.getInteger(JobParams.KEY_TASK_ID),
        //                JobParams.KEY_TASK_ID + " can not be null," + JsonUtil.toString(instanceParams));

        instanceParams.put(JobParams.KEY_TASK_ID, line.getOptionValue(JobParams.KEY_TASK_ID));
        instanceParams.put(IFullBuildContext.DRY_RUN, line.hasOption(IFullBuildContext.DRY_RUN));
        instanceParams.put(JobParams.KEY_COLLECTION, line.getOptionValue(JobParams.KEY_COLLECTION));
        instanceParams.put(DataxUtils.EXEC_TIMESTAMP, Long.parseLong(line.getOptionValue(DataxUtils.EXEC_TIMESTAMP)));
        return instanceParams;

        //        boolean dryRun = instanceParams.getBooleanValue(IFullBuildContext.DRY_RUN);
        //        String appName = instanceParams.getString(JobParams.KEY_COLLECTION);
        //        Long triggerTimestamp = instanceParams.getLong(DataxUtils.EXEC_TIMESTAMP);
        //        DefaultExecContext execChainContext = new DefaultExecContext(appName, triggerTimestamp);
        //        execChainContext.setCoordinator(ITISCoordinator.create());
        //        execChainContext.setDryRun(dryRun);
        //        execChainContext.setAttribute(JobCommon.KEY_TASK_ID, taskId);
        //        return execChainContext;
    }

    private static AbstractExecContext createDftExecContent(CommandLine line) {
        JSONObject instanceParams = deserializeInstanceParams(line);

        TriggersConfig triggersConfig = new TriggersConfig(instanceParams.getString(JobParams.KEY_COLLECTION), StoreResourceType.DataFlow);
        AbstractExecContext execContext = IExecChainContext.deserializeInstanceParams(triggersConfig, instanceParams);
//        execContext.setResType(StoreResourceType.DataFlow);
//        execContext.setWorkflowName(execContext.getIndexName());
        execContext.setExecutePhaseRange(new ExecutePhaseRange(FullbuildPhase.FullDump, FullbuildPhase.JOIN));

        Properties pts = line.getOptionProperties(DataXJoinProcessConsumer.Command.KEY_TABLE_GROUP);
        for (Map.Entry e : pts.entrySet()) {
            execContext.putTablePt(EntityName.parse(String.valueOf(e.getKey())), () -> String.valueOf(e.getValue()));
        }
        return execContext;

    }

    public static void executeJoin(RpcServiceReference feedback,
                                   AbstractExecContext execContext, SqlTaskNodeMeta sqlTask) {
        RpcUtils.setJoinStatus(execContext.getTaskId(), false, false, feedback, sqlTask.getExportName());
        IDataxProcessor dataxProc = execContext.getProcessor();
        // DataxProcessor.load(null, StoreResourceType.DataFlow,
        //        execContext.getIndexName());


        //SqlTaskNodeMeta.deserializeTaskNode(tskNodeReader);

        IDataxWriter writer = dataxProc.getWriter(null);
        if (!(writer instanceof IFlatTableBuilder)) {
            throw new IllegalStateException(writer.getClass() + " must be type of " + IFlatTableBuilder.class.getSimpleName());
        }
        IFlatTableBuilder flatTableBuilder = (IFlatTableBuilder) writer;


        IJoinTaskStatus joinTaskStatus = createJoinTaskStatus(sqlTask.getExportName());
        final IDataSourceFactoryGetter dsGetter = (IDataSourceFactoryGetter) writer;

        Supplier<IPrimaryTabFinder> primaryTabFinder = () -> {
            return new IPrimaryTabFinder() {
            };
        };

        ExecuteResult result = flatTableBuilder.startTask((ctx) -> {
            DataflowTask wfTsk = flatTableBuilder.createTask(sqlTask, false, execContext, ctx, joinTaskStatus,
                    dsGetter, primaryTabFinder);
            wfTsk.run();
            return (new ExecuteResult(true));
        });

        RpcUtils.setJoinStatus(execContext.getTaskId(), true, false, feedback, sqlTask.getExportName());
    }

    private static JoinPhaseStatus.JoinTaskStatus createJoinTaskStatus(String taskname) {
        if (StringUtils.isEmpty(taskname)) {
            throw new IllegalArgumentException("param taskname can not be null");
        }
        JoinPhaseStatus.JoinTaskStatus joinTaskStatus = new JoinPhaseStatus.JoinTaskStatus(taskname);
        return joinTaskStatus;
    }
}
