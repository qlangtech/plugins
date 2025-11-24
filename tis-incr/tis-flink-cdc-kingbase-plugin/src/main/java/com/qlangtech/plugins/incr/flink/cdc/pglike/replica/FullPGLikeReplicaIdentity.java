package com.qlangtech.plugins.incr.flink.cdc.pglike.replica;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.plugins.incr.flink.cdc.pglike.PGLikeReplicaIdentity;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.ds.IDataSourceFactoryGetter;
import com.qlangtech.tis.plugin.ds.ISelectedTab;
import com.qlangtech.tis.plugin.ds.JDBCConnection;
import com.qlangtech.tis.plugin.ds.postgresql.PGLikeDataSourceFactory;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import org.apache.commons.lang3.StringUtils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;

import static com.qlangtech.plugins.incr.flink.cdc.pglike.FlinkCDCPGLikeSourceFactory.FIELD_REPLICA_RULE;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2025/11/23
 */
public class FullPGLikeReplicaIdentity extends PGLikeReplicaIdentity {
    @Override
    public boolean isShallContainBeforeVals() {
        return true;
    }

    /**
     * FULL: 使用此值需要确保对应的表执行ALTER TABLE your_table_name REPLICA IDENTITY FULL;，表记录更新时会带上更新Before值，使用此方式比较耗费性能。
     *
     * @param msgHandler
     * @param context
     * @param dataSourceGetter
     * @param selectedTabs
     * @return
     */

    @Override
    public boolean validateSelectedTabs(
            IControlMsgHandler msgHandler
            , Context context
            , IDataSourceFactoryGetter dataSourceGetter, List<ISelectedTab> selectedTabs) {

        PGLikeDataSourceFactory ds = (PGLikeDataSourceFactory) dataSourceGetter.getDataSourceFactory();

        boolean[] validate = new boolean[]{true};
        ds.visitFirstConnection((conn) -> {
            validate[0] = validateReplicaIdentity(msgHandler, context, conn, ds.tabSchema, selectedTabs);
        });

        return validate[0];
    }

    /**
     * 验证所有选中的表是否配置了REPLICA IDENTITY FULL
     *
     * @param msgHandler   消息处理器
     * @param context      上下文
     * @param conn         数据库连接
     * @param selectedTabs 选中的表列表
     * @return 验证是否通过
     */
    private boolean validateReplicaIdentity(IControlMsgHandler msgHandler, Context context,
                                            JDBCConnection conn, final String tabSchema, List<ISelectedTab> selectedTabs) {
        if (StringUtils.isEmpty(tabSchema)) {
            throw new IllegalStateException("param tableSchema can not be empty");
        }
        boolean allValid = true;

        // SQL查询：检查表的REPLICA IDENTITY设置
        // relreplident: 'd' = default, 'f' = full, 'i' = index, 'n' = nothing
        String sql = "SELECT c.relreplident " +
                "FROM pg_class c " +
                "JOIN pg_namespace n ON c.relnamespace = n.oid " +
                "WHERE n.nspname = ? AND c.relname = ?";
        // String schema;
        try (PreparedStatement stmt = conn.preparedStatement(sql)) {
            for (ISelectedTab tab : selectedTabs) {
                String tableName = tab.getName();

                // 查询表的REPLICA IDENTITY设置
                stmt.setString(1, tabSchema);
                stmt.setString(2, tableName);

                try (ResultSet rs = stmt.executeQuery()) {
                    if (rs.next()) {
                        String replIdent = rs.getString("relreplident");

                        // 检查是否为FULL模式
                        if (!"f".equals(replIdent)) {
                            String fullTableName = tabSchema + "." + tableName;
                            String errMessage = "表'" + fullTableName + "'未配置REPLICA IDENTITY FULL，" +
                                    "请执行：ALTER TABLE " + fullTableName + " REPLICA IDENTITY FULL;";
                            msgHandler.addFieldError(context, FIELD_REPLICA_RULE, errMessage);
                            allValid = false;
                            return allValid;
                        }
                    } else {
                        // 表不存在
                        throw new IllegalStateException("table'" + tabSchema + "." + tableName + "' is not exist");
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException("the validator of replica is vailed", e);
        }

        return allValid;
    }


    @TISExtension
    public static final class DefaultDesc extends Descriptor<PGLikeReplicaIdentity> {
        @Override
        public String getDisplayName() {
            return FULL;
        }


    }
}
