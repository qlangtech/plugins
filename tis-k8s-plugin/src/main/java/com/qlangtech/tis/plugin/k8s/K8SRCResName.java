/**
 *   Licensed to the Apache Software Foundation (ASF) under one
 *   or more contributor license agreements.  See the NOTICE file
 *   distributed with this work for additional information
 *   regarding copyright ownership.  The ASF licenses this file
 *   to you under the Apache License, Version 2.0 (the
 *   "License"); you may not use this file except in compliance
 *   with the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */

package com.qlangtech.tis.plugin.k8s;

import com.qlangtech.tis.datax.job.DataXJobWorker.K8SWorkerCptType;
import com.qlangtech.tis.datax.job.OwnerJobResName;
import com.qlangtech.tis.datax.job.ServiceResName;
import org.apache.commons.lang.StringUtils;

import java.util.Optional;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-03-11 17:30
 **/
public class K8SRCResName<T> extends OwnerJobResName<T, NamespacedEventCallCriteria> {
    final Pattern patternTargetResource;
    final ServiceResName[] relevantSvc;

    public K8SRCResName(K8SWorkerCptType cptType, OwnerJobExec<T, NamespacedEventCallCriteria> subJobExec) {
        this(cptType.token, subJobExec);
    }

    public K8SRCResName(String name, OwnerJobExec<T, NamespacedEventCallCriteria> subJobExec) {
        this(name, subJobExec, new ServiceResName[0]);
    }

    public K8SRCResName(K8SWorkerCptType cptType, OwnerJobExec<T, NamespacedEventCallCriteria> subJobExec, ServiceResName... relevantSvc) {
        this(cptType.token, subJobExec, relevantSvc);
    }

    public K8SRCResName(String name, OwnerJobExec<T, NamespacedEventCallCriteria> subJobExec, ServiceResName... relevantSvc) {
        super(name, subJobExec);
        this.relevantSvc = relevantSvc;
        this.patternTargetResource = Pattern.compile("(" + this.getK8SResName() + ")\\-[a-z0-9]{1,}");
    }

    public boolean isPodMatch(String podName) {
        Matcher matcher = this.patternTargetResource.matcher(podName);
        return matcher.matches();
    }

    public ServiceResName[] getRelevantSvc() {
        return relevantSvc;
    }

    @Override
    protected String getResourceType() {
        return StringUtils.EMPTY;
    }

    public Optional<String> findPodResName(String msg) {
        Matcher matcher = this.patternTargetResource.matcher(msg);
        if (matcher.find()) {
            return Optional.of(matcher.group(0));
        }
        return Optional.empty();
    }
}
