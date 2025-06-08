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

package com.qlangtech.plugins.incr.flink.launch;

import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.PluginFormProperties;
import com.qlangtech.tis.extension.impl.PropertyType;
import com.qlangtech.tis.extension.impl.RootFormProperties;
import com.qlangtech.tis.extension.util.OverwriteProps;
import com.qlangtech.tis.manage.common.Option;
import org.apache.commons.lang3.EnumUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Triple;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.description.Description;
import org.apache.flink.configuration.description.HtmlFormatter;

import java.lang.reflect.Method;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-03-04 12:02
 **/
public class FlinkPropAssist<T extends Describable> {

    private Descriptor<T> descriptor;

    public FlinkPropAssist(Descriptor<T> descriptor) {
        this.descriptor = descriptor;
    }

    protected void addFieldDescriptor(String fieldName, ConfigOption<?> configOption) {
        addFieldDescriptor(fieldName, configOption, new OverwriteProps());
    }

    public static <T extends Describable, PLUGIN extends T> Options<PLUGIN> createOpts(Descriptor<T> descriptor) {
        FlinkPropAssist flinkProps = new FlinkPropAssist(descriptor);
        return flinkProps.createFlinkOptions();
    }

    protected Options createFlinkOptions() {
        return new Options(this);
    }

    public static class Options<T extends Describable> {
        private final List<Triple<String, TISFlinkProp, Function<T, Object>>> opts = Lists.newArrayList();
        private Map<String, /*** fieldname*/PropertyType> props;

        private FlinkPropAssist<T> propsAssist;

        public Options(FlinkPropAssist<T> propsAssist) {
            this.propsAssist = propsAssist;
//            this.props = getPluginFormPropertyTypes().accept(new PluginFormProperties.IVisitor() {
//                @Override
//                public Map<String, PropertyType> visit(RootFormProperties props) {
//                    return props.propertiesType;
//                }
//            });
        }

        public void addFieldDescriptor(String fieldName, ConfigOption<?> configOption) {
            propsAssist.addFieldDescriptor(fieldName, configOption);
        }

        public void addFieldDescriptor(String fieldName, ConfigOption<?> configOption, OverwriteProps overwriteProps) {
            propsAssist.addFieldDescriptor(fieldName, configOption, overwriteProps);
        }

        public org.apache.flink.configuration.Configuration createFlinkCfg(T instance) {
            org.apache.flink.configuration.Configuration cfg = new org.apache.flink.configuration.Configuration();
            PropertyType property = null;
            Function<T, Object> propGetter = null;
            Object val = null;
            for (Triple<String, TISFlinkProp, Function<T, Object>> opt : opts) {
                if (opt.getMiddle().overwriteProp.getDisabled()) {
                    continue;
                }
                propGetter = opt.getRight();
                if (propGetter != null) {
                    val = propGetter.apply(instance);
                } else {
                    if (StringUtils.isEmpty(opt.getLeft())) {
                        throw new IllegalStateException("fieldKey can not be empty");
                    }
                    property = Objects.requireNonNull(getProps().get(opt.getLeft())
                            , "key:" + opt.getLeft() + " relevant props can not be null");
                    val = property.getVal(false, instance);
                }
                if (val == null) {
                    continue;
                }
                cfg.set(opt.getMiddle().getConfigOpt(), val);
            }
            return cfg;
        }

        public void add(String fieldName, TISFlinkProp option) {
            this.add(fieldName, option, null);
        }

        public void add(ConfigOption option, Function<T, Object> propGetter) {
            this.add(null, TISFlinkProp.create(option), propGetter);
        }

        public void add(String fieldName, TISFlinkProp option, Function<T, Object> propGetter) {
            if (StringUtils.isNotEmpty(fieldName)) {
                this.addFieldDescriptor(fieldName, option.configOption, option.overwriteProp);

            }
            this.opts.add(Triple.of(fieldName, option, propGetter));
        }

        public Map<String, PropertyType> getProps() {
            if (props == null) {
                this.props = propsAssist.descriptor.getPluginFormPropertyTypes().accept(new PluginFormProperties.IVisitor() {
                    @Override
                    public Map<String, PropertyType> visit(RootFormProperties props) {
                        return props.propertiesType;
                    }
                });
            }
            return props;
        }
    }

    public static class TISFlinkProp {
        private OverwriteProps overwriteProp = new OverwriteProps();
        private final ConfigOption<?> configOption;

        boolean hasSetOverWrite = false;

        public static TISFlinkProp create(ConfigOption<?> configOption) {
            return new TISFlinkProp(configOption);
        }

        private TISFlinkProp(ConfigOption<?> configOption) {
            this.configOption = configOption;
        }

        public <T> ConfigOption<T> getConfigOpt() {
            return (ConfigOption<T>) configOption;
        }

        public TISFlinkProp overwriteDft(Object dftVal) {
            // overwriteProp.setDftVal(dftVal);
            return this.setOverwriteProp(OverwriteProps.dft(dftVal));
        }

        public TISFlinkProp overwritePlaceholder(Object placeholder) {
            return this.setOverwriteProp(OverwriteProps.placeholder(placeholder));
        }

        public TISFlinkProp setOverwriteProp(OverwriteProps overwriteProp) {
            if (hasSetOverWrite) {
                throw new IllegalStateException("overwriteProp has been setted ,can not be writen twice");
            }
            this.overwriteProp = overwriteProp;
            this.hasSetOverWrite = true;
            return this;
        }

        public TISFlinkProp disable() {
            OverwriteProps overwriteProps = new OverwriteProps();
            overwriteProps.setDisabled();
            return setOverwriteProp(overwriteProps);
        }
    }

    protected void addFieldDescriptor(String fieldName, ConfigOption<?> configOption, OverwriteProps overwriteProps) {
        Description desc = configOption.description();
        HtmlFormatter htmlFormatter = new HtmlFormatter();

        Object dftVal = overwriteProps.processDftVal(configOption.defaultValue());


        StringBuffer helperContent = new StringBuffer(htmlFormatter.format(desc));
        if (overwriteProps.appendHelper.isPresent()) {
            helperContent.append("\n\n").append(overwriteProps.appendHelper.get());
        }

        Class<?> targetClazz = getTargetClass(configOption);

        List<Option> opts = null;
        if (targetClazz == Duration.class) {
            if (dftVal != null) {
                dftVal = ((Duration) dftVal).getSeconds();
            }
            helperContent.append("\n\n 单位：`秒`");
        } else if (targetClazz == MemorySize.class) {
            if (dftVal != null) {
                dftVal = ((MemorySize) dftVal).getKibiBytes();
            }
            helperContent.append("\n\n 单位：`kb`");
        } else if (targetClazz.isEnum()) {
            List<Enum> enums = EnumUtils.getEnumList((Class<Enum>) targetClazz);
            opts = enums.stream().map((e) -> new Option(e.name())).collect(Collectors.toList());
        } else if (targetClazz == Boolean.class) {
            opts = Lists.newArrayList(new Option("是", true), new Option("否", false));
        }

        Optional<List<Option>> optsOp = overwriteProps.opts.isPresent() ? overwriteProps.opts : Optional.ofNullable(opts);

        descriptor.addFieldDescriptor(fieldName, dftVal, null, helperContent.toString()
                , optsOp, overwriteProps.getDisabled());
    }

    private static Method getClazzMethod;

    private static Class<?> getTargetClass(ConfigOption<?> configOption) {
        try {
            if (getClazzMethod == null) {
                getClazzMethod = ConfigOption.class.getDeclaredMethod("getClazz");
                getClazzMethod.setAccessible(true);
            }
            return (Class<?>) getClazzMethod.invoke(configOption);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

}
