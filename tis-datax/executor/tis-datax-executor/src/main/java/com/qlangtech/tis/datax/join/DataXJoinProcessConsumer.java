package com.qlangtech.tis.datax.join;

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.datax.DataXJobInfo;
import com.qlangtech.tis.datax.DataXJobRunEnvironmentParamsSetter;
import com.qlangtech.tis.datax.DataXJobSingleProcessorExecutor;
import com.qlangtech.tis.datax.DataXJobSubmit;
import com.qlangtech.tis.datax.DataxPrePostConsumer;
import com.qlangtech.tis.fullbuild.IFullBuildContext;
import com.qlangtech.tis.fullbuild.indexbuild.IDumpTable;
import com.qlangtech.tis.fullbuild.indexbuild.ITabPartition;
import com.qlangtech.tis.job.common.JobParams;
import com.qlangtech.tis.offline.DataxUtils;
import com.qlangtech.tis.sql.parser.ISqlTask;
import com.qlangtech.tis.sql.parser.TabPartitions;
import org.apache.commons.exec.CommandLine;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.util.List;
import java.util.Objects;

import static com.qlangtech.tis.sql.parser.ISqlTask.KEY_EXECUTE_TYPE;
import static com.qlangtech.tis.sql.parser.ISqlTask.KEY_EXPORT_NAME;
import static com.qlangtech.tis.sql.parser.ISqlTask.KEY_ID;
import static com.qlangtech.tis.sql.parser.ISqlTask.KEY_SQL_SCRIPT;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/12/7
 * @see DataxPrePostConsumer
 */
public class DataXJoinProcessConsumer extends DataXJobSingleProcessorExecutor<WorkflowHookMsg> {
    private final DataXJobRunEnvironmentParamsSetter.ExtraJavaSystemPramsSuppiler extraJavaSystemPramsSuppiler;

    public DataXJoinProcessConsumer(DataXJobRunEnvironmentParamsSetter.ExtraJavaSystemPramsSuppiler extraJavaSystemPramsSuppiler) {
        this.extraJavaSystemPramsSuppiler = extraJavaSystemPramsSuppiler;
    }

    @Override
    protected void addMainClassParams(WorkflowHookMsg msg, Integer taskId, String jobName, String resName,
                                      CommandLine cmdLine) {

        if (StringUtils.isEmpty(resName)) {
            throw new IllegalArgumentException("param resName can not be empty");
        }
        if (StringUtils.isEmpty(msg.getWorkflowName())) {
            throw new IllegalArgumentException("param msg.getWorkflowName()  can not be empty");
        }

        if (StringUtils.isEmpty(msg.getJobName())) {
            throw new IllegalArgumentException("param getJobName can not be empty");
        }

        Command command = new Command(cmdLine);

        JSONObject sqlTskJson = ISqlTask.json(msg.getSqlTask());

        command.add(KEY_ID, sqlTskJson.getString(KEY_ID));
       // command.add(KEY_SQL_SCRIPT, sqlTskJson.getString(KEY_SQL_SCRIPT));
        command.add(KEY_EXECUTE_TYPE, sqlTskJson.getString(KEY_EXECUTE_TYPE));
        command.add(KEY_EXPORT_NAME, sqlTskJson.getString(KEY_EXPORT_NAME));
//        command.add(KEY_DEPENDENCIES, Objects.requireNonNull(sqlTskJson.getString(KEY_DEPENDENCIES)
//                , "key:" + KEY_DEPENDENCIES + " relevant value can not be null"));

        command.add(JobParams.KEY_TASK_ID, String.valueOf(Objects.requireNonNull(taskId, "taskId can not be null")));
        command.add(IFullBuildContext.DRY_RUN, msg.getDryRun());
        command.add(JobParams.KEY_COLLECTION, msg.getWorkflowName());
        command.add(DataxUtils.EXEC_TIMESTAMP, String.valueOf(msg.getExecEpochMilli()));

        // command.add();

        //        cmdLine.addArgument(String.valueOf(Objects.requireNonNull(taskId, "taskId can not be null")));
        //        cmdLine.addArgument(msg.getWorkflowName());
        //        cmdLine.addArgument(getIncrStateCollectAddress());
        //        //cmdLine.addArgument(msg.getResType().getType());
        //        cmdLine.addArgument(msg.getJobName());
        //        cmdLine.addArgument(String.valueOf(msg.getExecEpochMilli()));


        TabPartitions dptPts = msg.getDptPts();
        dptPts.visitAll((entry) -> {
            IDumpTable tab = entry.getKey();
            ITabPartition pt = entry.getValue();
            command.add(tab, pt);
            // cmdLine.addArgument(tab.getFullName() + "_" + pt.getPt(), true);
        });

//        msg.getSqlTask();
//
//        List<DependencyNode> dependencyNodes = msg.getSqlTask().getDependencies();

    }

    @Override
    protected String[] getExtraJavaSystemPrams() {
        List<String> params = getExtraJavaSystemPramsSuppiler().get();
        return params.toArray(new String[params.size()]);
    }
    //    @Override
    //    protected String[] getExtraJavaSystemPrams() {
    //
    //        //        var.setName(Config.KEY_ASSEMBLE_HOST);
    //        //        var.setValue(processHost(Config.getAssembleHost()));
    //        //        envVars.add(var);
    //        //
    //        //        var = new V1EnvVar();
    //        //        var.setName(Config.KEY_TIS_HOST);
    //        //        var.setValue(processHost(Config.getTisHost()));
    //
    //        return new String[]{ //
    //                "-D" + Config.KEY_ASSEMBLE_HOST + "=" + Config.getAssembleHost() //
    //                , "-D" + Config.KEY_TIS_HOST + "=" + Config.getTisHost() //
    //                //        "-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=50000"
    //        };
    //
    //    }

    public DataXJobRunEnvironmentParamsSetter.ExtraJavaSystemPramsSuppiler getExtraJavaSystemPramsSuppiler() {
        return Objects.requireNonNull(extraJavaSystemPramsSuppiler, "extraJavaSystemPramsSuppiler can not be null");
    }

    public static class Command {
        public static final String KEY_TABLE_GROUP = "T";
        final CommandLine cmdLine;

        public Command(CommandLine cmdLine) {
            this.cmdLine = cmdLine;
        }

        public void add(String key, String val) {
            cmdLine.addArgument("-" + key, false);
            cmdLine.addArgument(val);
        }

        public void add(String key, Boolean val) {
            if (val) {
                cmdLine.addArgument("-" + key, false);
                // cmdLine.addArgument(val);
            }
        }

        public void add(IDumpTable tab, ITabPartition pt) {
            cmdLine.addArgument("-" + KEY_TABLE_GROUP + tab.getFullName() + "=" + pt.getPt(), false);
            //            cmdLine.addArgument(, true);
        }
    }

    @Override
    protected DataXJobSubmit.InstanceType getExecMode() {
        return null;
    }

    @Override
    protected String getClasspath() {
        return DataxPrePostConsumer.DEFAULT_CLASSPATH;
    }

    @Override
    protected File getWorkingDirectory() {
        return DataXJobInfo.getDataXExecutorDir();
    }

    @Override
    protected String getIncrStateCollectAddress() {
        return null;
    }

    @Override
    protected String getMainClassName() {
        return DataXJoinProcessExecutor.class.getName();
    }
}
