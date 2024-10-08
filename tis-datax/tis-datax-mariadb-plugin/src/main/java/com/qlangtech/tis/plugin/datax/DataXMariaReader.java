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

package com.qlangtech.tis.plugin.datax;

import com.qlangtech.tis.extension.TISExtension;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-09-14 09:46
 **/
public class DataXMariaReader extends DataxMySQLReader {

    @TISExtension()
    public static class MariaReaderDescriptor extends DefaultDescriptor {
        public MariaReaderDescriptor() {
            super();
        }

        @Override
        public String getDisplayName() {
            return MariaDBDataSourceFactory.DS_TYPE_MARIA_DB;
        }

        @Override
        public EndType getEndType() {
            return EndType.MariaDB;
        }
    }
}
