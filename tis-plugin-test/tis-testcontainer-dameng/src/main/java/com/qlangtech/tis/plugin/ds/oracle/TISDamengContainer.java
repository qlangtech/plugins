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

package com.qlangtech.tis.plugin.ds.oracle;

import org.testcontainers.containers.GenericContainer;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-10-01 09:38
 **/
public class TISDamengContainer extends GenericContainer {

    private static final int DAMENG_JDBC_PORT = 5236;

    public TISDamengContainer() {
        super(DamengDSFactoryContainer.DAMENG_DOCKER_IMAGE_NAME);
        //启动docker：docker run -d -p 30236:5236 --restart=always --name dm8_test --privileged=true
        // -e PAGE_SIZE=16
        // -e LD_LIBRARY_PATH=/opt/dmdbms/bin
        // -e  EXTENT_SIZE=32
        // -e BLANK_PAD_MODE=1
        // -e LOG_SIZE=1024
        // -e UNICODE_FLAG=1
        // -e LENGTH_IN_CHAR=1
        // -e INSTANCE_NAME=dm8_test
        // -v /data/dm8_test:/opt/dmdbms/data dm8_single:dm8_20230808_rev197096_x86_rh6_64
        this.addEnv("PAGE_SIZE", "16");
        this.addEnv("LD_LIBRARY_PATH", "/opt/dmdbms/bin");
        this.addEnv("EXTENT_SIZE", "32");
        this.addEnv("BLANK_PAD_MODE", "1");
        this.addEnv("LOG_SIZE", "1024");
        this.addEnv("UNICODE_FLAG", "1");
        this.addEnv("LENGTH_IN_CHAR", "1");
        this.addEnv("INSTANCE_NAME", "dm8_test");
        this.addExposedPorts(new int[]{DAMENG_JDBC_PORT});
    }


    public int getDamengJdbcMapperPort(){
        return this.getMappedPort(DAMENG_JDBC_PORT);
    }

}
