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

package com.qlangtech.tis.plugin.k8s;

import com.alibaba.fastjson.JSON;
import com.qlangtech.tis.lang.ErrorValue;
import com.qlangtech.tis.lang.TisException;
import com.qlangtech.tis.util.ClassloaderUtils;
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
        return convert(null, msg, e);
    }

    public static TisException convert(ErrorValue errCode, String msg, ApiException e) {

//        final ClassLoader current = Thread.currentThread().getContextClassLoader();
//        try {
//            Thread.currentThread().setContextClassLoader(V1Status.class.getClassLoader());
//            V1Status v1Status = JSON.parseObject(e.getResponseBody(), V1Status.class);
//            String errMsg = msg;
//            if (v1Status != null) {
//                errMsg = (msg == null) ? v1Status.getMessage() : msg + ":" + v1Status.getMessage();
//            }
//            return TisException.create(errCode, StringUtils.defaultIfEmpty(errMsg, e.getMessage()), e);
//
//        } finally {
//            Thread.currentThread().setContextClassLoader(current);
//        }

        try {
            return ClassloaderUtils.processByResetThreadClassloader(V1Status.class, () -> {
                V1Status v1Status = JSON.parseObject(e.getResponseBody(), V1Status.class);
                String errMsg = msg;
                if (v1Status != null) {
                    errMsg = (msg == null) ? v1Status.getMessage() : msg + ":" + v1Status.getMessage();
                }
                return TisException.create(errCode, StringUtils.defaultIfEmpty(errMsg, e.getMessage()), e);
            });
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }


    }
}
