package com.qlangtech.tis.plugin.datax.powerjob.impl.coresource;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.coredefine.module.action.impl.RcDeployment;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.powerjob.K8SDataXPowerJobServer;
import com.qlangtech.tis.plugin.datax.powerjob.PowerjobCoreDataSource;
import com.qlangtech.tis.plugin.ds.DBConfig;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.PostedDSProp;
import com.qlangtech.tis.plugin.k8s.K8SController;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import io.kubernetes.client.openapi.ApiException;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/10/31
 */
public class DefaultPowerjobCoreDataSource extends PowerjobCoreDataSource {
    private static final String FIELD_DB_NAME = "dbName";
    @FormField(ordinal = 0, type = FormFieldType.ENUM, validate = {Validator.require})
    public String dbName;

    @Override
    public String createCoreJdbcUrl() {
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

    @Override
    public RcDeployment getRCDeployment(K8SController k8SController) {
        return null;
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
            return true;
        }
    }

}
