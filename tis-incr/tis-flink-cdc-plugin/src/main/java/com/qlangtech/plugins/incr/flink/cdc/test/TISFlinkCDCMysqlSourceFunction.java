/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.plugins.incr.flink.cdc.test;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.async.message.client.consumer.IMQListener;
import com.qlangtech.tis.async.message.client.consumer.impl.MQListenerFactory;
import com.qlangtech.tis.datax.IDataxProcessor;
import com.qlangtech.tis.datax.ISelectedTab;
import com.qlangtech.tis.datax.impl.DataxProcessor;
import com.qlangtech.tis.datax.impl.ESTableAlias;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.manage.common.CenterResource;
import com.qlangtech.tis.plugin.ds.BasicDataSourceFactory;
import com.qlangtech.tis.plugin.ds.DBConfig;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import com.qlangtech.tis.plugin.incr.TISSinkFactory;
import com.qlangtech.tis.realtime.TISFlinkSourceHandle;
import com.qlangtech.tis.util.HeteroEnum;
import com.qlangtech.tis.util.IPluginContext;
import org.apache.commons.compress.utils.Lists;

import java.util.List;

//import com.qlangtech.tis.config.aliyun.IAliyunToken;
//import com.qlangtech.tis.plugin.datax.DataXElasticsearchWriter;
//import org.easymock.EasyMock;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-09-28 12:29
 **/
public class TISFlinkCDCMysqlSourceFunction {

    private final static String tabWaitinginstanceinfo = "waitinginstanceinfo";
    static final String dataxName = "mysql_elastic";

    public static void main(String[] args) throws Exception {

        CenterResource.setNotFetchFromCenterRepository();

        TISFlinkSourceHandle tisFlinkSourceHandle = new TISFlinkSourceHandle();

        // ElasticSearchSinkFactory esSinkFactory = new ElasticSearchSinkFactory();
        NamedPluginContext pluginContext = new NamedPluginContext(dataxName);
        List<TISSinkFactory> sinkFactories = TISSinkFactory.sinkFactory.getPlugins(pluginContext, null);
        if (sinkFactories.size() != 1) {
            throw new IllegalStateException("sinkFactories.size() must be 1");
        }
        // ElasticSearchSinkFactory esSinkFactory = (ElasticSearchSinkFactory) sinkFactories.get(0);

        tisFlinkSourceHandle.setSinkFuncFactory(sinkFactories.get(0));

        List<MQListenerFactory> mqFactories = HeteroEnum.MQ.getPlugins(pluginContext, null);
        if (mqFactories.size() != 1) {
            throw new IllegalStateException("mqFactories.size() must be 1");
        }
        for (MQListenerFactory mqFactory : mqFactories) {
            mqFactory.setConsumerHandle(tisFlinkSourceHandle);
        }

        MQListenerFactory mqFactory = mqFactories.get(0);

//        FlinkCDCMySQLSourceFactory factory = new FlinkCDCMySQLSourceFactory() {
//            @Override
//            public IConsumerHandle getConsumerHander() {
//                return tisFlinkSourceHandle;
//            }
//        };
        //factory.startupOptions = "latest";
        // com.qlangtech.plugins.incr.flink.cdc.mysql.FlinkCDCMysqlSourceFunction mq = (com.qlangtech.plugins.incr.flink.cdc.mysql.FlinkCDCMysqlSourceFunction) factory.create();

        IMQListener mq = mqFactory.create();

        IDataxProcessor dataXProcess = DataxProcessor.load(null, dataxName);//  EasyMock.createMock("dataXProcessor", IDataxProcessor.class);

        //  Map<String, IDataxProcessor.TableAlias> tableAlias = Maps.newHashMap();
        List<ISelectedTab.ColMeta> cols = Lists.newArrayList();
        ESTableAlias esTab = new ESTableAlias() {
            @Override
            public List<ISelectedTab.ColMeta> getSourceCols() {
                return cols;
            }
        };
        esTab.setFrom(tabWaitinginstanceinfo);
        esTab.setTo(tabWaitinginstanceinfo);

        cols.add(addCol("waitingorder_id", ISelectedTab.DataXReaderColType.STRING, true));
        cols.add(addCol("order_id", ISelectedTab.DataXReaderColType.STRING));
        cols.add(addCol("entity_id", ISelectedTab.DataXReaderColType.STRING));
        cols.add(addCol("is_valid", ISelectedTab.DataXReaderColType.INT));
        cols.add(addCol("last_ver", ISelectedTab.DataXReaderColType.INT));

        //esTab.setSourceCols(cols);
        // tableAlias.put(tabWaitinginstanceinfo, esTab);
        //  EasyMock.expect(dataXProcess.getTabAlias()).andReturn(tableAlias);

        //  DataXElasticsearchWriter esWriter = EasyMock.createMock("elasticWriter", DataXElasticsearchWriter.class);
        // EasyMock.expect(dataXProcess.getWriter(null)).andReturn(esWriter);

        // IAliyunToken httpToken = EasyMock.createMock("httpToken", IAliyunToken.class);

        // EasyMock.expect(httpToken.getEndpoint()).andReturn("http://192.168.28.201:9200");
        // EasyMock.expect(esWriter.getToken()).andReturn(httpToken);
        // EasyMock.expect(esWriter.getIndexName()).andReturn(tabWaitinginstanceinfo);

        // mqFactory.setDataXProcessor(dataXProcess);

        BasicDataSourceFactory dataSource = new BasicDataSourceFactory() {
            @Override
            public String buidJdbcUrl(DBConfig db, String ip, String dbName) {
                return null;
            }
        };
        dataSource.nodeDesc = "192.168.28.200[2],192.168.28.201[2]";
        dataSource.port = 3306;
        dataSource.dbName = "order";
        dataSource.userName = "root";
        dataSource.password = "123456";
        dataSource.encode = "utf8";


        List<ISelectedTab> tabs = Lists.newArrayList();
        TestSelectedTab tab = new TestSelectedTab(tabWaitinginstanceinfo, cols);
        tabs.add(tab);

        //  EasyMock.replay(dataXProcess, esWriter, httpToken);
        mq.start(dataSource, tabs, dataXProcess);


        // EasyMock.verify(dataXProcess, esWriter, httpToken);

    }

    private static ISelectedTab.ColMeta addCol(String colName, ISelectedTab.DataXReaderColType type) {
        return addCol(colName, type, false);
    }

    private static ISelectedTab.ColMeta addCol(String colName, ISelectedTab.DataXReaderColType type, boolean pk) {
        ISelectedTab.ColMeta col = new ISelectedTab.ColMeta();
        col.setName(colName);
        col.setType(type);
        col.setPk(pk);
        return col;
    }


    private static class NamedPluginContext implements IPluginContext {
        private final String collectionName;

        public NamedPluginContext(String collectionName) {
            this.collectionName = collectionName;
        }

        @Override
        public String getExecId() {
            return null;
        }

        @Override
        public boolean isCollectionAware() {
            return true;
        }

        @Override
        public boolean isDataSourceAware() {
            return false;
        }

        @Override
        public void addDb(Descriptor.ParseDescribable<DataSourceFactory> dbDesc, String dbName, Context context, boolean shallUpdateDB) {

        }

        @Override
        public String getCollectionName() {
            return this.collectionName;
        }

        @Override
        public void errorsPageShow(Context context) {

        }

        @Override
        public void addActionMessage(Context context, String msg) {

        }

        @Override
        public void setBizResult(Context context, Object result) {

        }

        @Override
        public void addErrorMessage(Context context, String msg) {

        }
    }
}
