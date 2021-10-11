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
package com.qlangtech.tis.dump.hive;

// import com.taobao.terminator.hdfs.client.router.SolrCloudPainRouter;
import org.apache.hadoop.hive.ql.exec.UDF;

/* *
 * @author 百岁（baisui@qlangtech.com）
 * @date 2016年3月30日
 */
public class SharedRouter extends UDF {

    // private static SolrCloudPainRouter cloudPainRouter;
    public String evaluate(final String shardValue, final String collection, final String runtime) {
        // return getRouter(collection, runtime).getShardIndex(shardValue);
        return null;
    }
    // private SolrCloudPainRouter getRouter(String collection, String runtime) {
    // if (cloudPainRouter == null) {
    // synchronized (SharedRouter.class) {
    // if (cloudPainRouter == null) {
    // try {
    // RunEnvironment.setSysRuntime(RunEnvironment.getEnum(runtime));
    // 
    // RealtimeTerminatorBeanFactory beanFactory = new RealtimeTerminatorBeanFactory();
    // beanFactory.setServiceName(collection);
    // beanFactory.setJustDump(false);
    // 
    // beanFactory.setIncrDumpProvider(new MockHDFSProvider());
    // beanFactory.setFullDumpProvider(new MockHDFSProvider());
    // beanFactory.setGrouprouter(null);
    // 
    // beanFactory.afterPropertiesSet();
    // 
    // cloudPainRouter = (SolrCloudPainRouter) beanFactory.getGrouprouter();
    // } catch (Exception e) {
    // throw new RuntimeException(e);
    // }
    // }
    // }
    // }
    // return cloudPainRouter;
    // 
    // }
}
