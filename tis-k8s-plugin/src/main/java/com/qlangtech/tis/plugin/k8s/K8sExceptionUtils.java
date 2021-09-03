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

package com.qlangtech.tis.plugin.k8s;

import com.alibaba.fastjson.JSON;
import com.qlangtech.tis.lang.TisException;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.models.V1Status;
import org.apache.commons.lang.StringUtils;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-07-28 21:13
 **/
public class K8sExceptionUtils {
    public static TisException convert(ApiException e) {
        return convert(null, e);
    }

    public static TisException convert(String msg, ApiException e) {
        V1Status v1Status = JSON.parseObject(e.getResponseBody(), V1Status.class);
        String errMsg = msg;
        if (v1Status != null) {
            errMsg = (msg == null) ? v1Status.getMessage() : msg + ":" + v1Status.getMessage();
        }
        return new TisException(StringUtils.defaultIfEmpty(errMsg, e.getMessage()), e);
    }
}
