/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.tis.plugins.flink.client;

import com.qlangtech.tis.realtime.transfer.DTO;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.net.URL;
import java.net.URLClassLoader;
import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-10-15 11:19
 **/
public class FlinkStreamDemo {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        SourceFunction<DTO> sourceFunc = createSourceFunction();

        DataStreamSource<DTO> dtoDataStreamSource = env.addSource(sourceFunc);

        SinkFunction<DTO> sinkFunction = createSink();

        dtoDataStreamSource.addSink(sinkFunction);

        env.execute("flink-example");
    }

    private static SinkFunction<DTO> createSink() {
        URL[] urls = new URL[]{};
        ClassLoader classLoader = new URLClassLoader(urls);
        ServiceLoader<ISinkFunctionFactory> loaders = ServiceLoader.load(ISinkFunctionFactory.class, classLoader);
        Iterator<ISinkFunctionFactory> it = loaders.iterator();
        if (it.hasNext()) {
            return it.next().create();
        }
        throw new IllegalStateException();
    }

    private static SourceFunction<DTO> createSourceFunction() {
        URL[] urls = new URL[]{};
        ClassLoader classLoader = new URLClassLoader(urls);
        ServiceLoader<ISourceFunctionFactory> loaders = ServiceLoader.load(ISourceFunctionFactory.class, classLoader);
        Iterator<ISourceFunctionFactory> it = loaders.iterator();
        if (it.hasNext()) {
            return it.next().create();
        }
        throw new IllegalStateException();
    }

    public interface ISinkFunctionFactory {
        SinkFunction<DTO> create();
    }

    public interface ISourceFunctionFactory {
        SourceFunction<DTO> create();
    }
}
