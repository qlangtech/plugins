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

package com.qlangtech.tis.plugin.datax.doris;

import com.qlangtech.tis.extension.Describable;

/**
 * https://doris.apache.org/zh-CN/docs/data-table/data-model/#duplicate-%E6%A8%A1%E5%9E%8B
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2024-02-28 10:28
 **/
public abstract class CreateTable implements Describable<CreateTable> {
  //  public abstract boolean isOff();

    public abstract String getKeyToken();
}
