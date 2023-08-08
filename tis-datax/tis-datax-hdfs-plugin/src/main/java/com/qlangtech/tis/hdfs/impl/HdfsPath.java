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
package com.qlangtech.tis.hdfs.impl;

import com.qlangtech.tis.fs.IPath;
import org.apache.hadoop.fs.Path;

/**
 * @author 百岁（baisui@qlangtech.com）
 * @date 2018年11月23日
 */
public class HdfsPath implements IPath {

    private final Path path;

    public HdfsPath(String path) {
        this(new Path(path));
    }

    public HdfsPath(Path path) {
        this.path = path;
    }

    @Override
    public String toString() {
        return this.path.toString();
    }

    /**
     * 取得文件名称
     *
     * @return
     */
    @Override
    public String getName() {
        return path.getName();
    }

    public HdfsPath(IPath parent, String name) {
        this.path = new Path(parent.unwrap(Path.class), name);
    }

    @Override
    public <T> T unwrap(Class<T> clazz) {
        if (clazz != Path.class) {
            throw new IllegalStateException(clazz + " is not type of " + Path.class);
        }
        return clazz.cast(this.path);
    }
}
