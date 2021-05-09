/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 *   This program is free software: you can use, redistribute, and/or modify
 *   it under the terms of the GNU Affero General Public License, version 3
 *   or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 *  This program is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *   FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.tis.plugin.datax;

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.datax.IDataxContext;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.impl.DataxWriter;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.extension.impl.IOUtils;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.ds.*;
import com.qlangtech.tis.plugin.ds.mysql.MySQLDataSourceFactory;
import org.apache.commons.collections.CollectionUtils;
import java.util.Optional;

/**
 * @author: baisui 百岁
 * @create: 2021-04-07 15:30
 **/
public class DataXElasticsearchWriter extends DataxWriter {
    private static final String DATAX_NAME = "Elasticsearch";

    @FormField(ordinal = 0, type = FormFieldType.INPUTTEXT, validate = { })
    public String endpoint;
        @FormField(ordinal = 1, type = FormFieldType.INPUTTEXT, validate = { })
    public String 描述：ElasticSearch的连接地址;
        @FormField(ordinal = 2, type = FormFieldType.INPUTTEXT, validate = { })
    public String 必选：是;
        @FormField(ordinal = 3, type = FormFieldType.INPUTTEXT, validate = { })
    public String 默认值：无;
        @FormField(ordinal = 4, type = FormFieldType.INPUTTEXT, validate = { })
    public String accessId;
        @FormField(ordinal = 5, type = FormFieldType.INPUTTEXT, validate = { })
    public String 描述：http auth中的user;
        @FormField(ordinal = 6, type = FormFieldType.INPUTTEXT, validate = { })
    public String 必选：否;
        @FormField(ordinal = 7, type = FormFieldType.INPUTTEXT, validate = { })
    public String 默认值：空;
        @FormField(ordinal = 8, type = FormFieldType.INPUTTEXT, validate = { })
    public String accessKey;
        @FormField(ordinal = 9, type = FormFieldType.INPUTTEXT, validate = { })
    public String 描述：http auth中的password;
        @FormField(ordinal = 10, type = FormFieldType.INPUTTEXT, validate = { })
    public String 必选：否;
        @FormField(ordinal = 11, type = FormFieldType.INPUTTEXT, validate = { })
    public String 默认值：空;
        @FormField(ordinal = 12, type = FormFieldType.INPUTTEXT, validate = { })
    public String index;
        @FormField(ordinal = 13, type = FormFieldType.INPUTTEXT, validate = { })
    public String 描述：elasticsearch中的index名;
        @FormField(ordinal = 14, type = FormFieldType.INPUTTEXT, validate = { })
    public String 必选：是;
        @FormField(ordinal = 15, type = FormFieldType.INPUTTEXT, validate = { })
    public String 默认值：无;
        @FormField(ordinal = 16, type = FormFieldType.INPUTTEXT, validate = { })
    public String type;
        @FormField(ordinal = 17, type = FormFieldType.INPUTTEXT, validate = { })
    public String 描述：elasticsearch中index的type名;
        @FormField(ordinal = 18, type = FormFieldType.INPUTTEXT, validate = { })
    public String 必选：否;
        @FormField(ordinal = 19, type = FormFieldType.INPUTTEXT, validate = { })
    public String 默认值：index名;
        @FormField(ordinal = 20, type = FormFieldType.INPUTTEXT, validate = { })
    public String cleanup;
        @FormField(ordinal = 21, type = FormFieldType.INPUTTEXT, validate = { })
    public String 描述：是否删除原表;
        @FormField(ordinal = 22, type = FormFieldType.INPUTTEXT, validate = { })
    public String 必选：否;
        @FormField(ordinal = 23, type = FormFieldType.INPUTTEXT, validate = { })
    public String 默认值：false;
        @FormField(ordinal = 24, type = FormFieldType.INPUTTEXT, validate = { })
    public String batchSize;
        @FormField(ordinal = 25, type = FormFieldType.INPUTTEXT, validate = { })
    public String 描述：每次批量数据的条数;
        @FormField(ordinal = 26, type = FormFieldType.INPUTTEXT, validate = { })
    public String 必选：否;
        @FormField(ordinal = 27, type = FormFieldType.INPUTTEXT, validate = { })
    public String 默认值：1000;
        @FormField(ordinal = 28, type = FormFieldType.INPUTTEXT, validate = { })
    public String trySize;
        @FormField(ordinal = 29, type = FormFieldType.INPUTTEXT, validate = { })
    public String 描述：失败后重试的次数;
        @FormField(ordinal = 30, type = FormFieldType.INPUTTEXT, validate = { })
    public String 必选：否;
        @FormField(ordinal = 31, type = FormFieldType.INPUTTEXT, validate = { })
    public String 默认值：30;
        @FormField(ordinal = 32, type = FormFieldType.INPUTTEXT, validate = { })
    public String timeout;
        @FormField(ordinal = 33, type = FormFieldType.INPUTTEXT, validate = { })
    public String 描述：客户端超时时间;
        @FormField(ordinal = 34, type = FormFieldType.INPUTTEXT, validate = { })
    public String 必选：否;
        @FormField(ordinal = 35, type = FormFieldType.INPUTTEXT, validate = { })
    public String 默认值：600000;
        @FormField(ordinal = 36, type = FormFieldType.INPUTTEXT, validate = { })
    public String discovery;
        @FormField(ordinal = 37, type = FormFieldType.INPUTTEXT, validate = { })
    public String 描述：启用节点发现将(轮询)并定期更新客户机中的服务器列表。;
        @FormField(ordinal = 38, type = FormFieldType.INPUTTEXT, validate = { })
    public String 必选：否;
        @FormField(ordinal = 39, type = FormFieldType.INPUTTEXT, validate = { })
    public String 默认值：false;
        @FormField(ordinal = 40, type = FormFieldType.INPUTTEXT, validate = { })
    public String compression;
        @FormField(ordinal = 41, type = FormFieldType.INPUTTEXT, validate = { })
    public String 描述：http请求，开启压缩;
        @FormField(ordinal = 42, type = FormFieldType.INPUTTEXT, validate = { })
    public String 必选：否;
        @FormField(ordinal = 43, type = FormFieldType.INPUTTEXT, validate = { })
    public String 默认值：true;
        @FormField(ordinal = 44, type = FormFieldType.INPUTTEXT, validate = { })
    public String multiThread;
        @FormField(ordinal = 45, type = FormFieldType.INPUTTEXT, validate = { })
    public String 描述：http请求，是否有多线程;
        @FormField(ordinal = 46, type = FormFieldType.INPUTTEXT, validate = { })
    public String 必选：否;
        @FormField(ordinal = 47, type = FormFieldType.INPUTTEXT, validate = { })
    public String 默认值：true;
        @FormField(ordinal = 48, type = FormFieldType.INPUTTEXT, validate = { })
    public String ignoreWriteError;
        @FormField(ordinal = 49, type = FormFieldType.INPUTTEXT, validate = { })
    public String 描述：忽略写入错误，不重试，继续写入;
        @FormField(ordinal = 50, type = FormFieldType.INPUTTEXT, validate = { })
    public String 必选：否;
        @FormField(ordinal = 51, type = FormFieldType.INPUTTEXT, validate = { })
    public String 默认值：false;
        @FormField(ordinal = 52, type = FormFieldType.INPUTTEXT, validate = { })
    public String ignoreParseError;
        @FormField(ordinal = 53, type = FormFieldType.INPUTTEXT, validate = { })
    public String 描述：忽略解析数据格式错误，继续写入;
        @FormField(ordinal = 54, type = FormFieldType.INPUTTEXT, validate = { })
    public String 必选：否;
        @FormField(ordinal = 55, type = FormFieldType.INPUTTEXT, validate = { })
    public String 默认值：true;
        @FormField(ordinal = 56, type = FormFieldType.INPUTTEXT, validate = { })
    public String alias;
        @FormField(ordinal = 57, type = FormFieldType.INPUTTEXT, validate = { })
    public String 描述：数据导入完成后写入别名;
        @FormField(ordinal = 58, type = FormFieldType.INPUTTEXT, validate = { })
    public String 必选：否;
        @FormField(ordinal = 59, type = FormFieldType.INPUTTEXT, validate = { })
    public String 默认值：无;
        @FormField(ordinal = 60, type = FormFieldType.INPUTTEXT, validate = { })
    public String aliasMode;
        @FormField(ordinal = 61, type = FormFieldType.INPUTTEXT, validate = { })
    public String 描述：数据导入完成后增加别名的模式，append(增加模式), exclusive(只留这一个);
        @FormField(ordinal = 62, type = FormFieldType.INPUTTEXT, validate = { })
    public String 必选：否;
        @FormField(ordinal = 63, type = FormFieldType.INPUTTEXT, validate = { })
    public String 默认值：append;
        @FormField(ordinal = 64, type = FormFieldType.INPUTTEXT, validate = { })
    public String settings;
        @FormField(ordinal = 65, type = FormFieldType.INPUTTEXT, validate = { })
    public String 描述：创建index时候的settings, 与elasticsearch官方相同;
        @FormField(ordinal = 66, type = FormFieldType.INPUTTEXT, validate = { })
    public String 必选：否;
        @FormField(ordinal = 67, type = FormFieldType.INPUTTEXT, validate = { })
    public String 默认值：无;
        @FormField(ordinal = 68, type = FormFieldType.INPUTTEXT, validate = { })
    public String splitter;
        @FormField(ordinal = 69, type = FormFieldType.INPUTTEXT, validate = { })
    public String 描述：如果插入数据是array，就使用指定分隔符;
        @FormField(ordinal = 70, type = FormFieldType.INPUTTEXT, validate = { })
    public String 必选：否;
        @FormField(ordinal = 71, type = FormFieldType.INPUTTEXT, validate = { })
    public String 默认值：-,-;
        @FormField(ordinal = 72, type = FormFieldType.INPUTTEXT, validate = { })
    public String column;
        @FormField(ordinal = 73, type = FormFieldType.INPUTTEXT, validate = { })
    public String 描述：elasticsearch所支持的字段类型，样例中包含了全部;
        @FormField(ordinal = 74, type = FormFieldType.INPUTTEXT, validate = { })
    public String 必选：是;
        @FormField(ordinal = 75, type = FormFieldType.INPUTTEXT, validate = { })
    public String dynamic;
        @FormField(ordinal = 76, type = FormFieldType.INPUTTEXT, validate = { })
    public String 描述: 不使用datax的mappings，使用es自己的自动mappings;
        @FormField(ordinal = 77, type = FormFieldType.INPUTTEXT, validate = { })
    public String 必选: 否;
        @FormField(ordinal = 78, type = FormFieldType.INPUTTEXT, validate = { })
    public String 默认值: false;
    
    @FormField(ordinal = 79, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String template;

    public static String getDftTemplate() {
        return IOUtils.loadResourceFromClasspath("DataXElasticsearchWriter-tpl.json");
    }


    @Override
    public String getTemplate() {
        return this.template;
    }

    @Override
    public IDataxContext getSubTask(Optional<IDataxProcessor.TableMap> tableMap) {
        if (!tableMap.isPresent()) {
            throw new IllegalArgumentException("param tableMap shall be present");
        }
        MySQLDataSourceFactory dsFactory = (MySQLDataSourceFactory) this.getDataSourceFactory();
        IDataxProcessor.TableMap tm = tableMap.get();
        if (CollectionUtils.isEmpty(tm.getSourceCols())) {
            throw new IllegalStateException("tablemap " + tm + " source cols can not be null");
        }
        TISTable table = new TISTable();
        table.setTableName(tm.getTo());
        DataDumpers dataDumpers = dsFactory.getDataDumpers(table);
        if (dataDumpers.splitCount > 1) {
            throw new IllegalStateException("dbSplit can not max than 1");
        }
        MySQLWriterContext context = new MySQLWriterContext();
        if (dataDumpers.dumpers.hasNext()) {
            IDataSourceDumper next = dataDumpers.dumpers.next();
            context.jdbcUrl = next.getDbHost();
            context.password = dsFactory.password;
            context.username = dsFactory.userName;
            context.tabName = table.getTableName();
            context.cols = tm.getSourceCols();
            context.dbName = this.dbName;
            context.writeMode = this.writeMode;
            context.preSql = this.preSql;
            context.postSql = this.postSql;
            context.session = session;
            context.batchSize = batchSize;
            return context;
        }

        throw new RuntimeException("dbName:" + dbName + " relevant DS is empty");
    }


    public static class MySQLWriterContext extends MySQLDataxContext {

        private String dbName;
        private String writeMode;
        private String preSql;
        private String postSql;
        private String session;
        private Integer batchSize;

        public String getDbName() {
            return dbName;
        }

        public String getWriteMode() {
            return writeMode;
        }

        public String getPreSql() {
            return preSql;
        }

        public String getPostSql() {
            return postSql;
        }

        public String getSession() {
            return session;
        }

        public Integer getBatchSize() {
            return batchSize;
        }
    }

    private DataSourceFactory getDataSourceFactory() {
        DataSourceFactoryPluginStore dsStore = TIS.getDataBasePluginStore(new PostedDSProp(this.dbName));
        return dsStore.getPlugin();
    }

    @TISExtension()
    public static class DefaultDescriptor extends Descriptor<DataxWriter> {
        public DefaultDescriptor() {
            super();
        }

        @Override
        public String getDisplayName() {
            return DATAX_NAME;
        }
    }
}
