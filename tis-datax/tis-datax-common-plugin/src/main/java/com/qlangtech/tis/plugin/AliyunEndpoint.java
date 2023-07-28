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

package com.qlangtech.tis.plugin;

import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.aliyun.AccessKey;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-07-27 00:12
 **/
public class AliyunEndpoint extends HttpEndpoint {


    public final AccessKey getAccessKey() {
        return this.authToken.accept(new AuthToken.Visitor<AccessKey>() {
            @Override
            public AccessKey visit(AccessKey accessKey) {
                return accessKey;
            }
        });
    }

    public static List<? extends Descriptor> filter(List<? extends Descriptor> descs) {
        if (CollectionUtils.isEmpty(descs)) {
            throw new IllegalArgumentException("param descs can not be null");
        }
        return descs.stream().filter((d) -> {
            return AccessKey.KEY_ACCESS.equals(d.getDisplayName());
        }).collect(Collectors.toList());
    }

    public String getEndpointHost() {
        return StringUtils.substringAfter(this.endpoint, "//");
    }

    @TISExtension()
    public static class DefaultDescriptor extends HttpEndpoint.DefaultDescriptor {
        @Override
        public String getDisplayName() {
            return KEY_FIELD_ALIYUN_TOKEN;
        }
    }
}
