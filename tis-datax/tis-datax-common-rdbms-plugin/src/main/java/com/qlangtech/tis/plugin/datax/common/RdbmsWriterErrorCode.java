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

package com.qlangtech.tis.plugin.datax.common;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-08-06 22:40
 **/
public enum  RdbmsWriterErrorCode implements ErrorCode {
    REQUIRED_DATAX_PARAM_ERROR("RdbmsWriter-01", "config param 'dataxName' is required"),
    REQUIRED_TABLE_NAME_PARAM_ERROR("RdbmsWriter-02", "config param 'tableName' is required");

    private final String code;
    private final String description;

    private RdbmsWriterErrorCode(String code, String description) {
        this.code = code;
        this.description = description;
    }

    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.description;
    }

    @Override
    public String toString() {
        return String.format("Code:[%s], Description:[%s].", this.code, this.description);
    }
}
