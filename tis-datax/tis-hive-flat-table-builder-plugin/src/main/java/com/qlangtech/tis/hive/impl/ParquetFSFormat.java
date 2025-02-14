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

package com.qlangtech.tis.hive.impl;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.hive.HdfsFileType;
import com.qlangtech.tis.plugin.datax.FSFormat;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;

/**
 * TODO： 先加着，具体到时候再实现
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-11-09 12:13
 **/
public class ParquetFSFormat extends FSFormat {
    public final static HdfsFileType parquetType = HdfsFileType.PARQUET;

    @Override
    public char getFieldDelimiter() {
        return ' ';
    }

    @Override
    public HdfsFileType getType() {
        return parquetType;
    }

    @TISExtension
    public static class DftDesc extends Descriptor<FSFormat> {
        @Override
        protected boolean validateAll(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            // return super.validateAll(msgHandler, context, postFormVals);
            //throw new NotImplementedException("sorry have not been implement yet, please wait ");
            ParquetFSFormat fsFormat = postFormVals.newInstance();
            fsFormat.getFieldDelimiter();
            return true;
        }

        @Override
        public String getDisplayName() {
            return parquetType.name();
        }
    }
}
