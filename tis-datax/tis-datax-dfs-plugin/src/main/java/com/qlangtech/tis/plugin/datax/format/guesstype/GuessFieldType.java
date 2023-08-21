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

package com.qlangtech.tis.plugin.datax.format.guesstype;

import com.alibaba.datax.plugin.unstructuredstorage.reader.UnstructuredReader;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.plugin.datax.format.BasicPainFormat;
import com.qlangtech.tis.plugin.ds.DataType;

import java.io.IOException;
import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-08-13 10:40
 **/
public class GuessFieldType implements Describable<GuessFieldType> {

    public void processGuess(DataType[] types, BasicPainFormat textFormat, UnstructuredReader reader) throws IOException {
        // DataType[] types = new DataType[colCount];
        // 最后将空缺的类型补充上
        isAllTypeJudged(types, Optional.of(DataType.createVarChar(32)));
    }

    protected boolean isAllTypeJudged(DataType[] types, Optional<DataType> dftType) {
        for (int i = 0; i < types.length; i++) {
            if (types[i] == null) {
                if (!dftType.isPresent()) {
                    return false;
                } else {
                    types[i] = dftType.get();
                }
            }
        }
        return true;
    }
}
