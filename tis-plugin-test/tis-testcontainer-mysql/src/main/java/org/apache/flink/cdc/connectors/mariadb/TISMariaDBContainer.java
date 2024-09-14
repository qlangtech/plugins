/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.cdc.connectors.mariadb;

import com.qlangtech.plugins.incr.flink.slf4j.TISLoggerConsumer;
import com.qlangtech.tis.TIS;
import com.qlangtech.tis.coredefine.module.action.TargetResName;
import com.qlangtech.tis.plugin.ds.DataSourceFactory;
import org.apache.flink.cdc.connectors.IDataSourceFactoryCreator;
import org.apache.flink.cdc.connectors.mysql.testutils.MySqlContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.MariaDBContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-09-13 13:14
 **/
public class TISMariaDBContainer extends MariaDBContainer<TISMariaDBContainer> implements IDataSourceFactoryCreator {
    private static final String SETUP_SQL_PARAM_NAME = "SETUP_SQL";
    private static Logger LOG = LoggerFactory.getLogger(TISMariaDBContainer.class);
//    public TISMariaDBContainer(String dockerImageName) {
//        super(dockerImageName);
//    }

    public TISMariaDBContainer(DockerImageName dockerImageName) {
        super(dockerImageName);
    }

    private static TISMariaDBContainer mariaDBContainer;

    public static TISMariaDBContainer createMariaDBContainer() {

        if (mariaDBContainer == null) {
            DockerImageName myImage = DockerImageName.parse("mariadb:10.11.8");
            //  .asCompatibleSubstituteFor("mariadb");
            mariaDBContainer = createMariaContainer(
                    myImage
                    , "maria/docker/server/tis"
                    , "maria/docker/column_type_test.sql");
        }


        return mariaDBContainer;
    }

    DataSourceFactory ds;

    @Override
    public DataSourceFactory createMySqlDataSourceFactory(TargetResName dataxName, boolean splitTabStrategy) {
        if (this.ds != null) {
            return this.ds;
        }
        return this.ds = MySqlContainer.getBasicDataSourceFactory(dataxName
                , TIS.get().getDescriptor("MariaDBDataSourceFactory"), this, splitTabStrategy);
    }

    @Override
    protected void configure() {
        if (parameters.containsKey(SETUP_SQL_PARAM_NAME)) {
            optionallyMapResourceParameterAsVolume(
                    SETUP_SQL_PARAM_NAME, "/docker-entrypoint-initdb.d/", "N/A");
        }
        super.configure();
    }

    public TISMariaDBContainer withSetupSQL(String sqlPath) {
        parameters.put(SETUP_SQL_PARAM_NAME, sqlPath);
        return this;
    }

    public static final TISMariaDBContainer createMariaContainer(DockerImageName dockerImageName, String myConf, String sqlClasspath) {
        TISMariaDBContainer container =
                new TISMariaDBContainer(dockerImageName)
                        .withConfigurationOverride(myConf)
                        .withSetupSQL(sqlClasspath)
                        .withDatabaseName("flink-test")
//                        .withUsername("flinkuser")
//                        .withPassword("flinkpw")
                        .withLogConsumer(new TISLoggerConsumer(LOG));
        ;


        //  .withLogConsumer(new TISLoggerConsumer(LOG));

        //container.withCopyFileToContainer(MountableFile.forClasspathResource(myConf), "/etc/mysql/my.cnf");
        //container.withCopyFileToContainer(MountableFile.forClasspathResource(sqlClasspath), "/docker-entrypoint-initdb.d/setup.sql");

//        container.copyFileToContainer(
//                Transferable.of(IOUtils.loadResourceFromClasspath(MySqlContainer.class
//                        , myConf, true, (i) -> org.apache.commons.io.IOUtils.toByteArray(i)))
//                , "/etc/mysql/my.cnf");
//
//        container.copyFileToContainer(
//                Transferable.of(IOUtils.loadResourceFromClasspath(MySqlContainer.class
//                        , sqlClasspath, true, (i) -> org.apache.commons.io.IOUtils.toByteArray(i)))
//                , "/docker-entrypoint-initdb.d/setup.sql");

        return container;
    }
}
