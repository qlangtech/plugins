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

package com.qlangtech.tis.plugin.datax.kingbase.dispatch;

import com.alibaba.citrus.turbine.Context;
import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import com.qlangtech.tis.plugin.datax.kingbase.KingBaseDispatch;
import com.qlangtech.tis.plugin.ds.kingbase.TISStubForKBProperty;
import com.qlangtech.tis.realtime.utils.NetUtils;
import com.qlangtech.tis.runtime.module.misc.IFieldErrorHandler;
import com.qlangtech.tis.runtime.module.misc.impl.AdapterFieldErrorHandler;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang.StringUtils;

import java.io.StringReader;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-02-06 18:35
 **/
public class On extends KingBaseDispatch {

    @FormField(ordinal = 0, type = FormFieldType.TEXTAREA, validate = {Validator.require})
    public String slavers;

    //LOADRATE
    @FormField(ordinal = 1, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer loadRate;


    @Override
    public void extractSetJdbcProps(Properties props) {

        List<KingBaseSlaver> nodes = parseSlaverNodes();

        props.setProperty(TISStubForKBProperty.USEDISPATCH, String.valueOf(Boolean.TRUE));
        props.setProperty(TISStubForKBProperty.SLAVE_ADD
                , nodes.stream().map((node) -> node.addr).collect(Collectors.joining(",")));
        props.setProperty(TISStubForKBProperty.SLAVE_PORT
                , nodes.stream().map((node) -> String.valueOf(node.port)).collect(Collectors.joining(",")));
        props.setProperty(TISStubForKBProperty.HOSTLOADRATE, String.valueOf(this.loadRate));
        props.setProperty(TISStubForKBProperty.USECONNECT_POOL, String.valueOf(Boolean.FALSE));
    }

    private List<KingBaseSlaver> parseSlaverNodes() {
        List<KingBaseSlaver> nodes = Lists.newArrayList();
        String[] tuple;
        try (StringReader lines = (new StringReader(this.slavers))) {
            LineIterator lineIt = IOUtils.lineIterator(lines);
            while (lineIt.hasNext()) {
                final String host = lineIt.nextLine();
                tuple = StringUtils.split(host, ":");
                if (tuple.length != 2) {
                    throw new IllegalStateException("illegal host address:" + host);
                }
                nodes.add(new KingBaseSlaver(tuple[0], Integer.parseInt(tuple[1])));
            }
        }
        return nodes;
    }

    private static class KingBaseSlaver {
        private final String addr;
        private final Integer port;

        public KingBaseSlaver(String addr, Integer port) {
            this.addr = addr;
            this.port = port;
        }
    }

    @TISExtension
    public static class DftDesc extends Descriptor<KingBaseDispatch> {
        public DftDesc() {
            super();
        }

        public boolean validateLoadRate(IFieldErrorHandler msgHandler, Context context, String fieldName, String val) {
            int rate = Integer.parseInt(val);
            if (rate > 100 || rate < 0) {
                msgHandler.addFieldError(context, fieldName, "必须为1到100之间");
                return false;
            }
            return true;
        }

        public boolean validateSlavers(IFieldErrorHandler msgHandler, Context context, String fieldName, String val) {
            final AtomicInteger lineNum = new AtomicInteger(1);
            try (StringReader lines = (new StringReader(val))) {
                LineIterator lineIt = IOUtils.lineIterator(lines);
                while (lineIt.hasNext()) {
                    final String host = lineIt.nextLine();
                    if (!Validator.host.validate(new AdapterFieldErrorHandler(msgHandler) {
                        @Override
                        public void addFieldError(Context context, String fieldName, String msg, Object... params) {
                            super.addFieldError(context, fieldName, "第" + lineNum.get() + "行，" + msg, params);
                        }
                    }, context, fieldName, host)) {
                        return false;
                    }
                    if (!NetUtils.isReachable(host)) {
                        msgHandler.addFieldError(context, fieldName, "第" + lineNum.get() + "行host:" + host + "不可连通");
                        return false;
                    }
                    lineNum.incrementAndGet();
                }
            } catch (Exception e) {
                throw new RuntimeException(e);
            }

            return true;
        }

        @Override
        public String getDisplayName() {
            return SWITCH_ON;
        }
    }
}
