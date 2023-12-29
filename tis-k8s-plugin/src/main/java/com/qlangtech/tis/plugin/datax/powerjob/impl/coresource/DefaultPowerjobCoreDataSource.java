package com.qlangtech.tis.plugin.datax.powerjob.impl.coresource;

import com.alibaba.citrus.turbine.Context;
import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.coredefine.module.action.impl.RcDeployment;
import com.qlangtech.tis.datax.TimeFormat;
import com.qlangtech.tis.datax.job.SSERunnable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.powerjob.K8SDataXPowerJobServer;
import com.qlangtech.tis.plugin.datax.powerjob.PowerjobCoreDataSource;
import com.qlangtech.tis.plugin.ds.DBConfig;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.DataSourceMeta.JDBCConnection;
import com.qlangtech.tis.plugin.ds.IDBAuthorizeTokenGetter;
import com.qlangtech.tis.plugin.ds.PostedDSProp;
import com.qlangtech.tis.plugin.ds.TableInDB;
import com.qlangtech.tis.plugin.k8s.K8SController;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import io.kubernetes.client.openapi.ApiException;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/10/31
 */
public class DefaultPowerjobCoreDataSource extends PowerjobCoreDataSource {
    private static final String FIELD_DB_NAME = "dbName";

    private static final String SQL_REPLACE_APP_INFO = "replace into app_info (id,app_name,gmt_create,gmt_modified,`password`) values (?,?,now(),now(),?)";
    private static final String SQL_SELECT_APP_INFO = "select `id`,`password` from app_info where app_name = ?";

    @FormField(ordinal = 0, type = FormFieldType.ENUM, validate = {Validator.require})
    public String dbName;

    @Override
    public void initialPowerjobAccount(K8SDataXPowerJobServer powerJobServer) {
        SSERunnable sse = SSERunnable.getLocal();
        DataSourceFactory ds = this.getDataSourceFactory();

        // powerJobServer.appName;

        ds.visitFirstConnection((conn) -> {
            AppNameHasRegister hasRegister = appNameHasRegister(powerJobServer, conn);
            if (hasRegister.dirty) {
                sse.info(K8SDataXPowerJobServer.K8S_DATAX_POWERJOB_REGISTER_ACCOUNT.getName()
                        , TimeFormat.getCurrentTimeStamp(), "app:" + powerJobServer.appName + " is dirty , replace with new account");
                try (PreparedStatement statement = conn.getConnection().prepareStatement(SQL_REPLACE_APP_INFO)) {
                    if (hasRegister.existId.isPresent()) {
                        statement.setInt(1, hasRegister.existId.get());
                    } else {
                        statement.setNull(1, Types.BIGINT);
                    }
                    statement.setString(2, powerJobServer.appName);
                    statement.setString(3, powerJobServer.password);

                    if (statement.executeUpdate() < 1) {
                        throw new IllegalStateException("replace statement is falid:" + SQL_REPLACE_APP_INFO + ",with appName:" + powerJobServer.appName);
                    }
                }
            } else {
                sse.info(K8SDataXPowerJobServer.K8S_DATAX_POWERJOB_REGISTER_ACCOUNT.getName()
                        , TimeFormat.getCurrentTimeStamp(), "app:" + powerJobServer.appName + " is exist ,skip create");
            }
        });
    }

    private AppNameHasRegister appNameHasRegister(K8SDataXPowerJobServer powerJobServer, JDBCConnection conn) throws SQLException {
        try (PreparedStatement statement = conn.getConnection().prepareStatement(SQL_SELECT_APP_INFO)) {
            statement.setString(1, powerJobServer.appName);
            try (ResultSet result = statement.executeQuery()) {
                if (result.next()) {
                    if (!StringUtils.equals(result.getString(2), powerJobServer.password)) {
                        // replace
                        return new AppNameHasRegister(true, Optional.of(result.getInt(1)));
                    }
                } else {
                    // replace
                    return new AppNameHasRegister(true, Optional.empty());
                }
            }
        }
        return new AppNameHasRegister(false, Optional.empty());
    }

    private static class AppNameHasRegister {
        // 老账户失效：不存在？密码不一致？
        private boolean dirty;
        private Optional<Integer> existId;

        public AppNameHasRegister(boolean dirty, Optional<Integer> existId) {
            this.dirty = dirty;
            this.existId = existId;
        }
    }

    @Override
    protected String getJdbcUrl() {
        DataSourceFactory ds = this.getDataSourceFactory();
        final DBConfig dbConfig = ds.getDbConfig();
        AtomicReference<String> jdbcUrlRef = new AtomicReference<>();
        try {
            dbConfig.vistDbName(new DBConfig.IProcess() {
                @Override
                public boolean visit(DBConfig config, String jdbcUrl, String ip, String dbName) throws Exception {
                    jdbcUrlRef.set(jdbcUrl);
                    return true;
                }
            });
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return Objects.requireNonNull(jdbcUrlRef.get(), "jdbcUrl can not be null");
    }

    private IDBAuthorizeTokenGetter getAuthorizeToken() {
        return IDBAuthorizeTokenGetter.get(this.getDataSourceFactory());
    }

    @Override
    protected String getExtractJdbcParams() {
        IDBAuthorizeTokenGetter tokenGetter = getAuthorizeToken();
        return KEY_USERNAME_AND_PASSWORD.format(new String[]{tokenGetter.getUserName(), tokenGetter.getPassword()});
    }

    @Override
    public RcDeployment getRCDeployment(K8SController k8SController) {
        return null;
    }


    @Override
    public void launchMetaStore(K8SDataXPowerJobServer powerJobServer) throws ApiException {
        // 不需要执行任何逻辑
    }

    @Override
    public void launchMetaStoreService(K8SDataXPowerJobServer powerJobServer) throws ApiException {
        // 不需要执行任何逻辑
    }

    public DataSourceFactory getDataSourceFactory() {
        return TIS.getDataBasePlugin(PostedDSProp.parse(this.dbName));
    }

    @TISExtension
    public static class DefaultDesc extends Descriptor<PowerjobCoreDataSource> {
        static final ArrayList<String> shallContainTabs
                = Lists.newArrayList("app_info", "container_info", "instance_info"
                , "job_info", "oms_lock", "server_info", "user_info"
                , "workflow_info", "workflow_instance_info", "workflow_node_info");

        @Override
        public String getDisplayName() {
            return "Customized";
        }

        @Override
        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            return super.validateAll(msgHandler, context, postFormVals);
        }

        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            // return super.validateAll(msgHandler, context, postFormVals);

            DefaultPowerjobCoreDataSource coreDs = postFormVals.newInstance();
            DataSourceFactory dseFactory = coreDs.getDataSourceFactory();
            if (!DataSourceFactory.DS_TYPE_MYSQL_V8.equals(dseFactory.getDescriptor().getDisplayName())) {
                msgHandler.addFieldError(context, FIELD_DB_NAME, "必须使用" + DataSourceFactory.DS_TYPE_MYSQL_V8 + "类型的数据源");
                return false;
            }

            TableInDB tabs = dseFactory.getTablesInDB();
            List<String> existTabs = tabs.getTabs();

            Set<String> lackTabs = Sets.newHashSet();
            for (String tab : shallContainTabs) {
                if (!existTabs.contains(tab)) {
                    lackTabs.add(tab);
                }
            }

            if (CollectionUtils.isNotEmpty(lackTabs)) {
                msgHandler.addFieldError(context, FIELD_DB_NAME, "库中缺少表:" + String.join(",", lackTabs));
                return false;
            }

            return true;
        }
    }

}
