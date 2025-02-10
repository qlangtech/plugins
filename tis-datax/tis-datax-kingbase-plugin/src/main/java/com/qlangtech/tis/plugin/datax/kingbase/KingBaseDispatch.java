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

package com.qlangtech.tis.plugin.datax.kingbase;

import com.qlangtech.tis.extension.Describable;

import java.io.Serializable;
import java.util.Properties;

/**
 * https://bbs.kingbase.com.cn/docHtml?recId=218c307e5f3d72bf20bb84a51859344a&url=aHR0cHM6Ly9iYnMua2luZ2Jhc2UuY29tLmNuL2tpbmdiYXNlLWRvYy92OS4xLjEuMjQvZmFxL2ZhcS1uZXcvaW50ZXJmYWNlL2pkYmMuaHRtbCNpZDQ
 *
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-02-06 18:34
 **/
public abstract class KingBaseDispatch implements Describable<KingBaseDispatch>, Serializable {
    public abstract void extractSetJdbcProps(Properties props);
}
