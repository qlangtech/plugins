package com.qlangtech.tis.plugin.datax.dameng.writer;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.SourceColMetaGetter;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.plugin.datax.CreateTableSqlBuilder;
import com.qlangtech.tis.plugin.datax.CreateTableSqlBuilder.ColWrapper;
import com.qlangtech.tis.plugin.datax.common.BasicDataXRdbmsWriter;
import com.qlangtech.tis.plugin.datax.dameng.ds.DaMengDataSourceFactory;
import com.qlangtech.tis.plugin.datax.dameng.reader.DataXDaMengReader;
import com.qlangtech.tis.plugin.datax.transformer.RecordTransformerRules;
import com.qlangtech.tis.plugin.ds.CMeta;
import com.qlangtech.tis.plugin.ds.ColumnMetaData;
import com.qlangtech.tis.plugin.ds.DataDumpers;
import com.qlangtech.tis.plugin.ds.DataType;
import com.qlangtech.tis.plugin.ds.IColMetaGetter;
import com.qlangtech.tis.plugin.ds.IDBReservedKeys;
import com.qlangtech.tis.plugin.ds.IDataSourceDumper;
import com.qlangtech.tis.plugin.ds.TISTable;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/9/14
 */
public class DataXDaMengWriter extends BasicDataXRdbmsWriter<DaMengDataSourceFactory> {
    private static final String DATAX_NAME = DataXDaMengReader.DATAX_NAME;
    private static final Logger logger = LoggerFactory.getLogger(DataXDaMengWriter.class);

//    @FormField(ordinal = 1, type = FormFieldType.ENUM, validate = {Validator.require})
//    public String writeMode;

    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath(DataXDaMengWriter.class, "dameng-writer-tpl.json");
    }


    @Override
    public IDataxContext getSubTask(Optional<IDataxProcessor.TableMap> tableMap, Optional<RecordTransformerRules> transformerRules) {
        if (!tableMap.isPresent()) {
            throw new IllegalArgumentException("param tableMap shall be present");
        }
        DaMengDataSourceFactory dsFactory = this.getDataSourceFactory();
        IDataxProcessor.TableMap tm = tableMap.get();
        if (CollectionUtils.isEmpty(tm.getSourceCols())) {
            throw new IllegalStateException("tablemap " + tm + " source cols can not be null");
        }
        TISTable table = new TISTable();
        table.setTableName(tm.getTo());
        DataDumpers dataDumpers = dsFactory.getDataDumpers(table);
        if (dataDumpers.splitCount > 1) {
            // 写入库还支持多组路由的方式分发，只能向一个目标库中写入
            throw new IllegalStateException("dbSplit can not max than 1");
        }
        DaMengWriterContext context = new DaMengWriterContext(this.dataXName);
        if (dataDumpers.dumpers.hasNext()) {
            IDataSourceDumper next = dataDumpers.dumpers.next();
            context.setJdbcUrl(next.getDbHost());
            context.setPassword(dsFactory.password);
            context.setUsername(dsFactory.userName);
            context.setTabName(table.getTableName());
            context.cols = IDataxProcessor.TabCols.create(new IDBReservedKeys() {
            }, tm, transformerRules);
            context.setDbName(this.dbName);
            //    context.writeMode = this.writeMode;
            context.setPreSql(this.preSql);
            context.setPostSql(this.postSql);
            context.setSession(session);
            context.setBatchSize(batchSize);
            return context;
        }

        throw new RuntimeException("dbName:" + dbName + " relevant DS is empty");
    }

    /**
     * https://eco.dameng.com/document/dm/zh-cn/pm/definition-statement.html#3.5.1%20%E8%A1%A8%E5%AE%9A%E4%B9%89%E8%AF%AD%E5%8F%A5
     * <p>
     * https://eco.dameng.com/document/dm/zh-cn/sql-dev/dmpl-sql-datatype.html#%E6%97%A5%E6%9C%9F%E6%97%B6%E9%97%B4%E6%95%B0%E6%8D%AE%E7%B1%BB%E5%9E%8B
     *
     * @return
     */
//    @Override
//    public CreateTableSqlBuilder.CreateDDL generateCreateDDL(SourceColMetaGetter colMetaGetter, IDataxProcessor.TableMap tableMapper, Optional<RecordTransformerRules> transformers) {
//
//        createDDL = createTableSqlBuilder.build();
//        return createDDL;
//    }


    @TISExtension()
    public static class DefaultDescriptor extends RdbmsWriterDescriptor {
        public DefaultDescriptor() {
            super();
        }


        @Override
        public boolean isSupportIncr() {
            return true;
        }

        @Override
        public boolean isSupportTabCreate() {
            return true;
        }

        @Override
        public IEndTypeGetter.EndType getEndType() {
            return EndType.DaMeng;
        }

        @Override
        public String getDisplayName() {
            return DATAX_NAME;
        }

        @Override
        protected boolean validatePostForm(IControlMsgHandler msgHandler, Context context, BasicDataXRdbmsWriter form) {

            DataXDaMengWriter dataxWriter = (DataXDaMengWriter) form;
            DaMengDataSourceFactory dsFactory = dataxWriter.getDataSourceFactory();
            if (dsFactory.splitTableStrategy.isSplittable()) {
                msgHandler.addFieldError(context, KEY_DB_NAME_FIELD_NAME, "Writer端不能使用带有分表策略的数据源");
                return false;
            }

            return super.validatePostForm(msgHandler, context, dataxWriter);
        }
    }
}
