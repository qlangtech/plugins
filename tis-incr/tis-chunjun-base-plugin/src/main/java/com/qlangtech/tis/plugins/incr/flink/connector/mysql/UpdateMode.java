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

package com.qlangtech.tis.plugins.incr.flink.connector.mysql;

import com.qlangtech.tis.TIS;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.IPropertyType;
import com.qlangtech.tis.extension.PluginFormProperties;

import java.util.Map;
import java.util.Optional;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-07-18 10:04
 **/
public abstract class UpdateMode implements Describable<UpdateMode> {


    public void set(Map<String, Object> params) {
        params.put("writeMode", getMode());
    }

    protected abstract String getMode();

    @Override
    public Descriptor<UpdateMode> getDescriptor() {
        Descriptor<UpdateMode> desc = TIS.get().getDescriptor(this.getClass());
        if (!(desc instanceof BasicDescriptor)) {
            throw new IllegalStateException("desc must be type of " + BasicDescriptor.class.getName());
        }
        return desc;
    }

    protected static abstract class BasicDescriptor extends Descriptor<UpdateMode> {
        public final PluginFormProperties getPluginFormPropertyTypes(Optional<IPropertyType.SubFormFilter> subFormFilter) {
            return super.getPluginFormPropertyTypes(Optional.empty());
        }
    }
}
