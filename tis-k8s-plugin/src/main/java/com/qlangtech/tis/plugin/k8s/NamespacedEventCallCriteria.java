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

import io.kubernetes.client.openapi.models.V1ObjectMeta;
import io.kubernetes.client.openapi.models.V1ReplicationController;
import org.apache.commons.lang.StringUtils;

import java.util.Objects;

/**
 * 用于在watch Event 阶段查看 event事件过滤用
 */
public abstract class NamespacedEventCallCriteria {
    /**
     * 如果是pod，ownerid 则为该Pod 隶属的 rc 的uid
     */
    private final String ownerUid;
    private final String ownerName;

    protected NamespacedEventCallCriteria(String ownerUid, String ownerName) {
        this.ownerUid = ownerUid;
        this.ownerName = ownerName;
    }

    public final String getOwnerName() {
        return ownerName;
    }

    public final String getOwnerUid() {
        return ownerUid;
    }

    public String getLabelSelector() {
        return null;
    }

    public String getResourceVersion() {
        return null;
    }
//
//        public NamespacedEventCallCriteria(String val) {
//            if (StringUtils.isEmpty(val)) {
//                throw new IllegalArgumentException("param val can not be null");
//            }
//            this.val = val;
//        }
//
//        public String getVer() {
//            return val;
//        }

    public static NamespacedEventCallCriteria createResVersion(String ownerUid, String ownerName, final String resourceVersion) {

        if (StringUtils.isEmpty(resourceVersion)) {
            throw new IllegalArgumentException("resourceVersion can not be empty");
        }
        if (StringUtils.isEmpty(ownerUid)) {
            throw new IllegalArgumentException("ownerUid can not be empty");
        }
        if (StringUtils.isEmpty(ownerName)) {
            throw new IllegalArgumentException("ownerName can not be be empty");
        }
        return new NamespacedEventCallCriteria(ownerUid, ownerName) {
            @Override
            public String getResourceVersion() {
                return resourceVersion;// Objects.requireNonNull(newRC, "newRC can not be null").getMetadata().getResourceVersion();
            }
        };
    }

    public static NamespacedEventCallCriteria createResVersion(V1ObjectMeta metadata) {
        Objects.requireNonNull(metadata, "metadata can not be null");
        return createResVersion(metadata.getUid(), metadata.getName(), metadata.getResourceVersion());
    }

    public static NamespacedEventCallCriteria createResVersion(V1ReplicationController newRC) {
        V1ObjectMeta metadata = Objects.requireNonNull(newRC, "newRC can not be null").getMetadata();
        return createResVersion(metadata.getUid(), metadata.getName(), metadata.getResourceVersion());
//        return new NamespacedEventCallCriteria() {
//            @Override
//            public String getResourceVersion() {
//                return Objects.requireNonNull(newRC, "newRC can not be null").getMetadata().getResourceVersion();
//            }
//        };
    }

//    public static AppTimestampLabelSelector createAppTimestampLabelSelector(TargetResName name, long timestamp) {
//        return new AppTimestampLabelSelector(name, timestamp);
//    }


//    public static class AppTimestampLabelSelector extends NamespacedEventCallCriteria {
//        private final String timestamp;
//
//        public AppTimestampLabelSelector(TargetResName name, long timestamp) {
//            this.timestamp = name.getK8SResName() + timestamp;
//        }
//
//        @Override
//        public String getLabelSelector() {
//            return K8SUtils.LABEL_APP_TIMESTAMP + "=" + timestamp;
//        }
//
//        public void setSelectLabel(Map<String, String> labes) {
//            labes.put(K8SUtils.LABEL_APP_TIMESTAMP, timestamp);
//        }
//
//        @Override
//        public String toString() {
//            return this.getLabelSelector();
//        }
//    }
}
