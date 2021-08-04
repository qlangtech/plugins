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
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;

import java.sql.Connection;
import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-05-28 10:50
 **/
public class DefaultHiveConnGetter extends ParamsConfig implements IHiveConnGetter {

    private static final String PLUGIN_NAME = "HiveConn";

    public static final String KEY_HIVE_ADDRESS = "hiveAddress";
    public static final String KEY_USE_USERTOKEN = "useUserToken";
    public static final String KEY_USER_NAME = "userName";
    public static final String KEY_PASSWORD = "password";

    @FormField(ordinal = 0, validate = {Validator.require, Validator.identity}, identity = true)
    public String name;

    @Override
    public String identityValue() {
        return this.name;
    }

    @FormField(ordinal = 1, validate = {Validator.require, Validator.host})
    public String // "192.168.28.200:10000";
            hiveAddress;
    @FormField(ordinal = 2, validate = {Validator.require, Validator.db_col_name})
    public String dbName;

    @FormField(ordinal = 3, validate = {Validator.require}, type = FormFieldType.ENUM)
    public boolean useUserToken;

    @FormField(ordinal = 4, type = FormFieldType.INPUTTEXT, validate = {Validator.db_col_name})
    public String userName;

    @FormField(ordinal = 5, type = FormFieldType.PASSWORD, validate = {})
    public String password;

    @Override
    public String getDbName() {
        return this.dbName;
    }

    @Override
    public Connection createConfigInstance() {
        try {

            Optional<HiveDBUtils.UserToken> userToken = this.useUserToken
                    ? Optional.of(new HiveDBUtils.UserToken(this.userName, this.password)) : Optional.empty();

            return HiveDBUtils.getInstance(this.hiveAddress, this.dbName, userToken).createConnection();
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
        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            return HiveFlatTableBuilder.validateHiveAvailable(msgHandler, context, postFormVals);
        }

        @Override
        public String getDisplayName() {
            return PLUGIN_NAME;
        }
    }


}
