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

package com.qlangtech.tis.plugin.datax.powerjob.impl;

import com.qlangtech.tis.trigger.jst.ILogListener;
import com.qlangtech.tis.trigger.socket.ExecuteState;

import java.io.IOException;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-12-27 08:59
 **/
public abstract class PowerJobPodLogListener implements ILogListener {
    @Override
    public final void sendMsg2Client(Object biz) throws IOException {
        ExecuteState<String> log = (ExecuteState<String>) biz;
        this.consumePodLogMsg(log);
    }

    protected abstract void consumePodLogMsg(ExecuteState<String> log);

    @Override
    public final void read(Object event) {

    }

    @Override
    public final boolean isClosed() {
        return false;
    }
}
