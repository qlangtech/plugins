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

package com.qlangtech.tis.hive;

import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.config.ParamsConfig;
import com.qlangtech.tis.config.hive.IHiveConnGetter;
import com.qlangtech.tis.dump.hive.HiveDBUtils;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.offline.flattable.HiveFlatTableBuilder;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;

import java.sql.Connection;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-28 10:50
 **/
public class DefaultHiveConnGetter extends ParamsConfig implements IHiveConnGetter {

    private static final String PLUGIN_NAME = "HiveConn";

    @FormField(ordinal = 0, validate = {Validator.require, Validator.identity}, identity = true)
    public String name;

    @FormField(ordinal = 1, validate = {Validator.require, Validator.host})
    public String // "192.168.28.200:10000";
            hiveAddress;
    @FormField(ordinal = 2, validate = {Validator.require, Validator.db_col_name})
    public String dbName;

    @Override
    public String getDbName() {
        return this.dbName;
    }

    @Override
    public Connection createConfigInstance() {
        try {
            return HiveDBUtils.getInstance(this.hiveAddress, this.dbName).createConnection();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }

    @TISExtension()
    public static class DefaultDescriptor extends Descriptor<ParamsConfig> {
        public DefaultDescriptor() {
            super();
            // this.registerSelectOptions(HiveFlatTableBuilder.KEY_FIELD_NAME, () -> TIS.getPluginStore(FileSystemFactory.class).getPlugins());
        }

        @Override
        protected boolean validate(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            return HiveFlatTableBuilder.validateHiveAvailable(msgHandler, context, postFormVals);
            //return super.validate(msgHandler, context, postFormVals);
        }

        @Override
        public String getDisplayName() {
            return PLUGIN_NAME;
        }
    }


}
