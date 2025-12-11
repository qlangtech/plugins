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

package com.qlangtech.tis.plugin.ds.kingbase;

import com.kingbase8.KBProperty;

/**
 * Stub for KBProperty constants from com.kingbase8.KBProperty.
 * These constant values are hardcoded to avoid runtime dependency on kingbase8 driver.
 *
 * Original values extracted from:
 * - KBProperty.HOSTLOADRATE.getName() = "HOSTLOADRATE"
 * - KBProperty.USECONNECT_POOL.getName() = "USECONNECT_POOL"
 * - KBProperty.SLAVE_PORT.getName() = "SLAVE_PORT"
 * - KBProperty.USEDISPATCH.getName() = "USEDISPATCH"
 * - KBProperty.CLIENT_ENCODING.getName() = "clientEncoding"
 * - KBProperty.SLAVE_ADD.getName() = "SLAVE_ADD"
 *
 * NOTE: If KingBase driver updates these property names in the future,
 * these values need to be manually updated accordingly.
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2025/12/11
 */
public class TISStubForKBProperty {

    /**
     * Property name for host load rate.
     * Extracted from KBProperty.HOSTLOADRATE.getName()
     */
    public static final String HOSTLOADRATE = "HOSTLOADRATE";

    /**
     * Property name for connection pool usage.
     * Extracted from KBProperty.USECONNECT_POOL.getName()
     * @see  KBProperty#USECONNECT_POOL
     */

    public static final String USECONNECT_POOL = "USECONNECT_POOL";

    /**
     * Property name for slave port.
     * Extracted from KBProperty.SLAVE_PORT.getName()
     * @see  KBProperty#SLAVE_PORT
     */
    public static final String SLAVE_PORT = "SLAVE_PORT";

    /**
     * Property name for dispatch usage.
     * Extracted from KBProperty.USEDISPATCH.getName()
     */
    public static final String USEDISPATCH = "USEDISPATCH";

    /**
     * Property name for client encoding.
     * Extracted from KBProperty.CLIENT_ENCODING.getName()
     */
    public static final String CLIENT_ENCODING = "clientEncoding";

    /**
     * Property name for slave address.
     * Extracted from KBProperty.SLAVE_ADD.getName()
     */
    public static final String SLAVE_ADD = "SLAVE_ADD";

    /**
     * Private constructor to prevent instantiation.
     */
    private TISStubForKBProperty() {
        throw new AssertionError("Utility class should not be instantiated");
    }
}
