package com.qlangtech.tis.plugin.datax.transformer.impl;

import com.alibaba.citrus.turbine.Context;
import com.alibaba.datax.common.element.ColumnAwareRecord;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.MultiStepsSupportHost;
import com.qlangtech.tis.extension.MultiStepsSupportHostDescriptor;
import com.qlangtech.tis.extension.OneStepOfMultiSteps;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.plugin.IPluginStore;
import com.qlangtech.tis.plugin.datax.SelectedTab;
import com.qlangtech.tis.plugin.datax.transformer.InParamer;
import com.qlangtech.tis.plugin.datax.transformer.OutputParameter;
import com.qlangtech.tis.plugin.datax.transformer.UDFDefinition;
import com.qlangtech.tis.plugin.datax.transformer.UDFDesc;
import com.qlangtech.tis.plugin.datax.transformer.impl.joiner.JoinerSelectDataSource;
import com.qlangtech.tis.plugin.datax.transformer.impl.joiner.JoinerSelectTable;
import com.qlangtech.tis.plugin.datax.transformer.impl.joiner.JoinerSetMatchConditionAndCols;
import com.qlangtech.tis.plugin.datax.transformer.impl.joiner.TargetRowsCache;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.DBConfig;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.ds.JDBCConnection;
import com.qlangtech.tis.plugin.table.join.TableJoinFilterCondition;
import com.qlangtech.tis.plugin.table.join.TableJoinFilterConditionCreatorFactory;
import com.qlangtech.tis.plugin.table.join.TableJoinMatchCondition;
import com.qlangtech.tis.plugin.table.join.TableJoinMatchConditionCreatorFactory;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

/**
 * Joiner UDF for joining primary table with dimension table
 * <p>
 * This UDF implements a multi-step configuration process:
 * <ol>
 *   <li>Step 1: Select data source ({@link JoinerSelectDataSource})</li>
 *   <li>Step 2: Select target table ({@link JoinerSelectTable})</li>
 *   <li>Step 3: Set match conditions and columns ({@link JoinerSetMatchConditionAndCols})</li>
 * </ol>
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/1/13
 * @see MultiStepsSupportHost
 * @see JoinerSelectDataSource
 * @see JoinerSelectTable
 * @see JoinerSetMatchConditionAndCols
 */
public class JoinerUDF extends UDFDefinition implements MultiStepsSupportHost, IPluginStore.AfterPluginSaved {
    private static final Logger logger = LoggerFactory.getLogger(JoinerUDF.class);
    private OneStepOfMultiSteps[] stepsPlugin;

    private transient int errorCount;
    private transient StringBuffer _selectSQL;

    @Override
    public OneStepOfMultiSteps[] getMultiStepsSavedItems() {
        return this.stepsPlugin;
    }

    @Override
    public List<OutputParameter> outParameters() {
        JoinerSetMatchConditionAndCols matchConditionAndCols = this.getJoinerSetMatchConditionAndCols();
        List<OutputParameter> outParams = Lists.newArrayList();
        final String prefix = getOutputColPrefix(matchConditionAndCols);
        for (CMeta tcol : matchConditionAndCols.targetCols) {
            // FIXME tcol.getType() 这里的type应该null的
            outParams.add(OutputParameter.create(prefix + tcol.getName(), true, tcol.getType()));
        }
        return outParams;
    }

    @Override
    public List<InParamer> inParameters() {
        return Lists.newArrayList();
    }

    @Override
    public void evaluate(ColumnAwareRecord record) {

        JoinerSelectDataSource joinerSelectDataSource = this.getJoinerSelectDataSource();
        JoinerSelectTable joinerSelectTable = this.getJoinerSelectTable();
        JoinerSetMatchConditionAndCols joinerSetMatchConditionAndCols = this.getJoinerSetMatchConditionAndCols();
        DataSourceFactory dataSource = joinerSelectDataSource.getDataSourceFactory();

        TargetRowsCache.JoinCacheKey cacheKey = new TargetRowsCache.JoinCacheKey();
        try {
            /**
             * 指定主表与子表的关联条件
             */
            List<TableJoinMatchCondition> matchCondition = joinerSetMatchConditionAndCols.matchCondition;
            if (CollectionUtils.isEmpty(matchCondition)) {
                throw new IllegalStateException("matchCondition can not be empty");
            }


            for (TableJoinMatchCondition mc : matchCondition) {
                // 从主表中拿到记录值
                Object valPrimary = record.getColumn(mc.getPrimaryTableMatchColName());
                cacheKey.addParam(mc.getDimensionMatchColName()).addPrimaryVal(valPrimary);
            }

            /**
             * 指定主表或者子表的过滤条件
             */
            List<TableJoinFilterCondition> filterConditions = joinerSetMatchConditionAndCols.filterConditions;
            for (TableJoinFilterCondition fc : filterConditions) {
                if (fc.getTableType() == TableJoinFilterConditionCreatorFactory.TableType.Dimension) {
                    cacheKey.addParam(fc.getColumnName()).addParam(fc.getValue());
                }
            }

            TargetRowsCache.JoinCacheValue exist = null;
            if (joinerSelectTable.cache.isOn()) {
                exist = joinerSelectTable.cache.getFromCache(cacheKey);
                if (exist == null) {
                    // 缓存中没有，不存在
                    exist = selectFromDB(dataSource, matchCondition, cacheKey, joinerSelectTable.tagetTable, joinerSetMatchConditionAndCols);
                    joinerSelectTable.cache.set2Cache(cacheKey, exist);
                }
            } else {
                exist = selectFromDB(dataSource, matchCondition, cacheKey, joinerSelectTable.tagetTable, joinerSetMatchConditionAndCols);
            }

            if (exist.isNull()) {
                // 缓存实例为空直接退出
                return;
            }
            final String prefix = getOutputColPrefix(joinerSetMatchConditionAndCols);
            Object colVal = null;
            for (CMeta tc : joinerSetMatchConditionAndCols.targetCols) {
                colVal = exist.get(tc.getName());
                if (colVal != null) {
                    record.setColumn(prefix + tc.getName(), colVal);
                }
            }
        } catch (Throwable e) {
            if (joinerSetMatchConditionAndCols.skipError) {
                if (((errorCount++) % 100) == 0) {
                    // 100个错误打印一个日志
                    logger.warn(String.valueOf(cacheKey), e);
                    return;
                }
            } else {
                throw new RuntimeException(e);
            }
        }

    }

    private static String getOutputColPrefix(JoinerSetMatchConditionAndCols joinerSetMatchConditionAndCols) {
        return StringUtils.trimToEmpty(joinerSetMatchConditionAndCols.colPrefix);
    }

    @Override
    public void afterSaved(IPluginContext pluginContext, Optional<Context> context) {
        this._selectSQL = null;
        this._jdbcConnection = null;
        this.errorCount = 0;
    }

    /**
     * 从数据库中加载
     *
     * @param dataSource
     * @param matchCondition
     * @param cacheKey
     * @param tagetTable
     * @param joinerSetMatchConditionAndCols
     * @return
     */
    private TargetRowsCache.JoinCacheValue  //
    selectFromDB( //
                  DataSourceFactory dataSource //
            , List<TableJoinMatchCondition> matchCondition, TargetRowsCache.JoinCacheKey cacheKey //
            , String tagetTable, JoinerSetMatchConditionAndCols joinerSetMatchConditionAndCols) {

        List<CMeta> targetCols = joinerSetMatchConditionAndCols.targetCols;
        JDBCConnection connection = this.getJdbcConnection(dataSource);
        StringBuffer selectSQL = getSelectSQL(dataSource, matchCondition, tagetTable, joinerSetMatchConditionAndCols, targetCols);

        try {
            TargetRowsCache.JoinCacheValue queryResult = new TargetRowsCache.JoinCacheValue();
            queryResult.setNull(true);
            TableJoinMatchCondition mc = null;
            TableJoinMatchCondition.PreparedStatementSetter preparedStatementSetter = null;
            if (cacheKey.getPrimaryValsLength() != matchCondition.size()) {
                throw new IllegalStateException("cacheKey.getPrimaryValsLength()=" //
                        + cacheKey.getPrimaryValsLength() + " != matchCondition.size():" + matchCondition.size());
            }
            try (PreparedStatement preparedStatement = connection.preparedStatement(selectSQL.toString())) {

                for (int index = 0; index < matchCondition.size(); index++) {
                    mc = matchCondition.get(index);
                    preparedStatementSetter = mc.preparedStatementSetter();
                    preparedStatementSetter.setVal(preparedStatement, index //
                            , Objects.requireNonNull(cacheKey.getPrimaryVal(index) //
                                    , "index:" + index + ",match condition:" + mc.getDimensionMatchColName()));
                    //  mc.setVal(preparedStatement, paramIndex, cacheKey.getPrimaryVal());
                }

                try (ResultSet resultSet = preparedStatement.executeQuery()) {
                    if (resultSet.next()) {
                        queryResult.setNull(false);
                        int colNum = 1;
                        Object colVal = null;
                        for (CMeta targetCol : targetCols) {
                            colVal = resultSet.getObject(colNum++);
                            if (colVal != null) {
                                queryResult.put(targetCol.getName(), colVal);
                            }
                        }
                    }
                }
            }

            return queryResult;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private StringBuffer getSelectSQL(DataSourceFactory dataSource, List<TableJoinMatchCondition> matchCondition, String tagetTable, JoinerSetMatchConditionAndCols joinerSetMatchConditionAndCols, List<CMeta> targetCols) {
        if (this._selectSQL == null) {
            _selectSQL = new StringBuffer();
            _selectSQL.append("SELECT ");
            _selectSQL.append(targetCols.stream() //
                    .map((col) -> dataSource.getEscapedEntity(col.getName())).collect(Collectors.joining(",")));
            _selectSQL.append(" FROM ").append(dataSource.getEscapedEntity(tagetTable));
            _selectSQL.append(" WHERE ");

            boolean first = true;
            for (TableJoinMatchCondition mc : matchCondition) {
                if (!first) {
                    _selectSQL.append(" AND ");
                } else {
                    first = false;
                }
                // TODO:这里如果是非数字类型的话，需要对valPrimary加单引号处理
                _selectSQL.append(dataSource.getEscapedEntity(mc.getDimensionMatchColName()) + " = ?");
            }

            /**
             * 指定主表或者子表的过滤条件
             */
            List<TableJoinFilterCondition> filterConditions = joinerSetMatchConditionAndCols.filterConditions;
            for (TableJoinFilterCondition fc : filterConditions) {
                if (fc.getTableType() == TableJoinFilterConditionCreatorFactory.TableType.Dimension) {
                    // cacheKey.addParam(fc.getColumnName()).addParam(fc.getValue());
                    _selectSQL.append(" AND ").append(dataSource.getEscapedEntity(fc.getColumnName())).append(fc.getOperator().getToken());
                    switch (fc.getValueType()) {
                        case NUMBER:
                        case BOOLEAN: {
                            _selectSQL.append(fc.getValue());
                            break;
                        }
                        case STRING: {
                            _selectSQL.append("'").append(fc.getValue()).append("'");
                            break;
                        }
                    }
                }
            }
        }

        return _selectSQL;
    }

    private transient JDBCConnection _jdbcConnection;

    /**
     * 创建 jdbc连接
     *
     * @param dataSourceFactory
     * @return
     */
    private JDBCConnection getJdbcConnection(DataSourceFactory dataSourceFactory) {

        if (this._jdbcConnection == null) {
            // DataSourceFactory dataSourceFactory = joinerSelectDataSource.getDataSourceFactory();
            DBConfig dbConfig = dataSourceFactory.getDbConfig();
            AtomicReference<String> jdbcUrlRef = new AtomicReference<>();
            try {
                dbConfig.vistDbName(new DBConfig.IProcess() {
                    @Override
                    public boolean visit(DBConfig config, String jdbcUrl, String ip, String dbName) throws Exception {
                        jdbcUrlRef.set(jdbcUrl);
                        return true;
                    }
                });
                if (jdbcUrlRef.get() == null) {
                    throw new IllegalStateException("jdbcUrlRef can not be null");
                }
                return this._jdbcConnection = dataSourceFactory.getConnection(jdbcUrlRef.get(), Optional.empty(), false);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }

        return this._jdbcConnection;
    }

    /**
     * Maximum number of columns to display in the description
     */
    private static final int MAX_DISPLAY_COLS = 5;

    /**
     * Get the literia (description) of this UDF for display purposes
     * <p>
     * This method builds a human-readable description of the join configuration,
     * including selected columns, data source, match conditions, and cache settings.
     *
     * @return List of UDFDesc describing the join configuration
     */
    @Override
    public List<UDFDesc> getLiteria() {
        // Get all step configurations
        JoinerSetMatchConditionAndCols conditionAndCols = this.getJoinerSetMatchConditionAndCols();
        JoinerSelectTable selectTable = this.getJoinerSelectTable();
        JoinerSelectDataSource selectDS = this.getJoinerSelectDataSource();

        List<UDFDesc> literia = Lists.newArrayList();

        // Add select information
        literia.add(buildSelectDesc(conditionAndCols, selectDS, selectTable));

        // Add match conditions
        literia.add(buildMatchConditionDesc(conditionAndCols, selectTable));

        // Add filter conditions if present
        if (CollectionUtils.isNotEmpty(conditionAndCols.filterConditions)) {
            literia.add(buildFilterConditionDesc(conditionAndCols, selectTable));
        }

        // Add cache status
        // isOn() ? "enable" : "disabled"
        literia.add(new UDFDesc("Cache", (selectTable.cache.getUDFDesc())));

        return literia;
    }

    private JoinerSelectDataSource getJoinerSelectDataSource() {
        JoinerSelectDataSource selectDS = this.getOneStepOf(OneStepOfMultiSteps.Step.Step1);
        return selectDS;
    }

    private JoinerSelectTable getJoinerSelectTable() {
        JoinerSelectTable selectTable = this.getOneStepOf(OneStepOfMultiSteps.Step.Step2);
        return selectTable;
    }

    private JoinerSetMatchConditionAndCols getJoinerSetMatchConditionAndCols() {
        JoinerSetMatchConditionAndCols conditionAndCols = this.getOneStepOf(OneStepOfMultiSteps.Step.Step3);
        return conditionAndCols;
    }

    /**
     * Build the select description including columns, data source, and prefix
     *
     * @param conditionAndCols Configuration from step 3
     * @param selectDS         Data source from step 1
     * @return UDFDesc for select information
     */
    private UDFDesc buildSelectDesc(JoinerSetMatchConditionAndCols conditionAndCols,
                                    JoinerSelectDataSource selectDS, JoinerSelectTable selectTable) {
        List<UDFDesc> selectDesc = Lists.newArrayList();

        // Format columns display
        String colsDisplay = formatColumnsDisplay(conditionAndCols.targetCols);
        selectDesc.add(new UDFDesc("Cols", colsDisplay));
        selectDesc.add(new UDFDesc("Join Table", selectTable.tagetTable));
        selectDesc.add(new UDFDesc("DataSource", selectDS.dbName));
        selectDesc.add(new UDFDesc("Prefix", conditionAndCols.colPrefix));

        return new UDFDesc("Select", selectDesc);
    }

    /**
     * Format columns for display
     * Shows up to MAX_DISPLAY_COLS columns, with ellipsis if there are more
     *
     * @param cols List of columns
     * @return Formatted string like "'col1','col2','col3'...total 10 columns"
     */
    private String formatColumnsDisplay(List<CMeta> cols) {
        int totalCols = cols.size();
        String colsDisplay = cols.stream()
                .limit(MAX_DISPLAY_COLS)
                .map(col -> "'" + col.getName() + "'")
                .collect(Collectors.joining(","));

        if (totalCols > MAX_DISPLAY_COLS) {
            colsDisplay += String.format("...total %d columns", totalCols);
        }

        return colsDisplay;
    }

    /**
     * Build match condition descriptions
     *
     * @param conditionAndCols Configuration from step 3
     * @param selectTable      Table selection from step 2
     * @return UDFDesc for match conditions
     */
    private UDFDesc buildMatchConditionDesc(JoinerSetMatchConditionAndCols conditionAndCols,
                                            JoinerSelectTable selectTable) {
        AtomicInteger conditionNum = new AtomicInteger(0);
        List<UDFDesc> matchConditions = conditionAndCols.matchCondition.stream()
                .map(mc -> new UDFDesc(
                        "Condition" + conditionNum.incrementAndGet(),
                        String.format("%s=%s.%s",
                                mc.getPrimaryTableMatchColName(),
                                selectTable.tagetTable,
                                mc.getDimensionMatchColName())))
                .collect(Collectors.toList());

        return new UDFDesc("With Match", matchConditions);
    }

    /**
     * Build filter condition descriptions
     *
     * @param conditionAndCols Configuration from step 3
     * @param selectTable      Table selection from step 2
     * @return UDFDesc for filter conditions
     */
    private UDFDesc buildFilterConditionDesc(JoinerSetMatchConditionAndCols conditionAndCols,
                                             JoinerSelectTable selectTable) {
        AtomicInteger filterNum = new AtomicInteger(0);
        List<UDFDesc> filterConditions = conditionAndCols.filterConditions.stream()
                .map(fc -> {
                    String tableName = TableJoinFilterConditionCreatorFactory.TableType.Primary == (fc.getTableType()) ?
                            "PrimaryTable" : selectTable.tagetTable;
                    String valueDisplay = formatFilterValue(fc);
                    return new UDFDesc(
                            "Filter" + filterNum.incrementAndGet(),
                            String.format("%s.%s %s %s",
                                    tableName,
                                    fc.getColumnName(),
                                    fc.getOperator().getToken(),
                                    valueDisplay));
                })
                .collect(Collectors.toList());

        return new UDFDesc("With Filter", filterConditions);
    }

    /**
     * Format filter value for display based on value type
     *
     * @param fc Filter condition
     * @return Formatted value string
     */
    private String formatFilterValue(TableJoinFilterCondition fc) {
        if (TableJoinFilterConditionCreatorFactory.ValueType.STRING == (fc.getValueType())) {
            return "'" + fc.getValue() + "'";
        }
        return fc.getValue();
    }

    /**
     * Set all step plugins to this host
     * <p>
     * This method is called by the framework to inject all step configurations
     * after user completes the multi-step wizard.
     *
     * @param stepsPlugin Array of step plugins in order
     */
    @Override
    public void setSteps(OneStepOfMultiSteps[] stepsPlugin) {
        this.stepsPlugin = stepsPlugin;
        final int FIXED_JOIN_STEPS_LENGTH = 3;
        if (stepsPlugin.length != FIXED_JOIN_STEPS_LENGTH) {
            throw new IllegalStateException("stepsPlugin.length must be equal to" + FIXED_JOIN_STEPS_LENGTH);
        }
        JoinerSelectTable preSavedStepPlugin = (JoinerSelectTable) stepsPlugin[OneStepOfMultiSteps.Step.Step2.getStepIndex()];
        JoinerSetMatchConditionAndCols matchCondition = (JoinerSetMatchConditionAndCols) stepsPlugin[OneStepOfMultiSteps.Step.Step3.getStepIndex()];
        List<ColumnMetaData> columnMetaData = preSavedStepPlugin.reflectTabCols(stepsPlugin);
        Map<String, ColumnMetaData> colRegister = columnMetaData.stream().collect(Collectors.toMap(Option::getName, (col) -> col));
        ColumnMetaData c = null;
        for (CMeta col : matchCondition.targetCols) {
            c = colRegister.get(col.getName());
            if (c == null) {
                throw new IllegalStateException("col:" + col.getName() + " relevant col meta can not be null");
            }
            // 服务端再根据列名重新设置类型
            col.setType(c.getType());
        }
        // 设置维度表匹配列设置类型
        for (TableJoinMatchCondition mc : matchCondition.matchCondition) {
            c = colRegister.get(mc.getDimensionMatchColName());
            if (c == null) {
                throw new IllegalStateException("col:" + mc.getDimensionMatchColName() + " relevant col meta can not be null");
            }
            mc.setDimensionMatchColType(c.getType());
        }
    }


    /**
     * Descriptor for JoinerUDF
     * <p>
     * Defines the multi-step configuration process for joining tables.
     */
    @TISExtension
    public static class DefaultDescriptor extends UDFDefinition.BasicUDFDesc implements MultiStepsSupportHostDescriptor<JoinerUDF> {
        public DefaultDescriptor() {
            super();
        }

        @Override
        public EndType getTransformerEndType() {
            return EndType.Constant;
        }


        @Override
        public String getDisplayName() {
            return "Joiner Outer Table";
        }

        @Override
        public Class<JoinerUDF> getHostClass() {
            return JoinerUDF.class;
        }

        /**
         * Get the list of step descriptions for the multi-step wizard
         * <p>
         * Defines three steps:
         * <ol>
         *   <li>Select data source</li>
         *   <li>Select target table</li>
         *   <li>Set match conditions and columns</li>
         * </ol>
         *
         * @return List of step descriptions
         */
        @Override
        public List<OneStepOfMultiSteps.BasicDesc> getStepDescriptionList() {
            return Lists.newArrayList(new JoinerSelectDataSource.Desc()
                    , new JoinerSelectTable.Desc()
                    , new JoinerSetMatchConditionAndCols.Desc());
        }

        /**
         * Append external properties to the multi-step configuration
         * <p>
         * Adds source table columns to the context so they can be used
         * in match condition UI.
         *
         * @param multiStepsCfg Configuration object to append properties to
         * @throws IllegalStateException if source table columns are not available
         */
        @Override
        public void appendExternalProps(JSONObject multiStepsCfg) {
            List<CMeta> sourceTabCols = SelectedTab.getSelectedCols();
            if (CollectionUtils.isEmpty(sourceTabCols)) {
                throw new IllegalStateException("sourceTabCols can not be empty");
            }
            multiStepsCfg.put(TableJoinMatchConditionCreatorFactory.KEY_SOURCE_TAB_COLS, sourceTabCols);
        }
    }

}
