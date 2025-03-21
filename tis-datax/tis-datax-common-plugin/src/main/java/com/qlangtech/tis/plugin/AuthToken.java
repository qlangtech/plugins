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

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.plugin.aliyun.NoneToken;
import com.qlangtech.tis.plugin.aliyun.UsernamePassword;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-10-31 09:51
 **/
public abstract class AuthToken implements Describable<AuthToken> {
    public static final String KEY_ACCESS = "accessKey";
    public interface IAliyunAccessKey {
        public String getAccessKeyId();

        public String getAccessKeySecret();
    }

    public abstract <T> T accept(Visitor<T> visitor);


    public interface Visitor<T> {

        default public T visit(NoneToken noneToken) {
            throw new UnsupportedOperationException();
        }

        default public T visit(IAliyunAccessKey accessKey) {
            throw new UnsupportedOperationException();
        }

        default public T visit(UsernamePassword accessKey) {
            throw new UnsupportedOperationException();
        }
    }

}
