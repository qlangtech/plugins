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

package com.qlangtech.tis.plugin.datax.kingbase.mode;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.IEndTypeGetter.EndType;
import com.qlangtech.tis.plugin.datax.kingbase.KingBaseCompatibleMode;

import java.util.Optional;

import static com.qlangtech.tis.plugin.ds.BasicDataSourceFactory.PG_ESCAPE_COL_CHAR;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-01-14 15:26
 **/
public class PGMode extends KingBaseCompatibleMode {
    private static final EndType endType = EndType.Postgres;

    @Override
    public Optional<String> getEscapeChar() {
        return PG_ESCAPE_COL_CHAR;
    }

    @Override
    public EndType getEndType() {
        return endType;
    }

    @TISExtension
    public static class DftDesc extends Descriptor<KingBaseCompatibleMode> {
        public DftDesc() {
            super();
        }

        @Override
        public String getDisplayName() {
            return EndType.Postgres.name();
        }
    }
}
