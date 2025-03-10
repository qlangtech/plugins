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

package com.qlangtech.plugins.incr.debuzium;

import com.google.common.collect.Lists;
import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.PluginFormProperties;
import com.qlangtech.tis.extension.impl.PropertyType;
import com.qlangtech.tis.extension.impl.RootFormProperties;
import com.qlangtech.tis.extension.util.OverwriteProps;
import com.qlangtech.tis.manage.common.Option;
import io.debezium.config.Field;
import io.debezium.config.Field.EnumRecommender;

import org.apache.commons.lang.StringUtils;
import org.apache.kafka.common.config.ConfigDef.Type;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2022-03-04 12:02
 **/
public class DebuziumPropAssist<T extends Describable> {

    private Descriptor<T> descriptor;

    private DebuziumPropAssist(Descriptor<T> descriptor) {
        this.descriptor = descriptor;
    }

    protected void addFieldDescriptor(String fieldName, Field configOption) {
        addFieldDescriptor(fieldName, configOption, new OverwriteProps());
    }

    public static <T extends Describable, PLUGIN extends T> Options<PLUGIN> createOpts(Descriptor<T> descriptor) {
        DebuziumPropAssist flinkProps = new DebuziumPropAssist(descriptor);
        return flinkProps.createFlinkOptions();
    }

    protected Options createFlinkOptions() {
        return new Options(this);
    }

    public static class Options<T extends Describable> {
        private final List<FieldTriple<T>> opts = Lists.newArrayList();
        private Map<String, /*** fieldname*/PropertyType> props;

        private DebuziumPropAssist<T> propsAssist;

        public Options(DebuziumPropAssist<T> propsAssist) {
            this.propsAssist = propsAssist;
//            this.props = getPluginFormPropertyTypes().accept(new PluginFormProperties.IVisitor() {
//                @Override
//                public Map<String, PropertyType> visit(RootFormProperties props) {
//                    return props.propertiesType;
//                }
//            });
        }

        public void addFieldDescriptor(String fieldName, Field configOption) {
            propsAssist.addFieldDescriptor(fieldName, configOption);
        }

        public void addFieldDescriptor(String fieldName, Field configOption, OverwriteProps overwriteProps) {
            propsAssist.addFieldDescriptor(fieldName, configOption, overwriteProps);
        }

//        public org.apache.flink.configuration.Configuration createFlinkCfg(T instance) {
//            org.apache.flink.configuration.Configuration cfg = new org.apache.flink.configuration.Configuration();
//            PropertyType property = null;
//            Function<T, Object> propGetter = null;
//            Object val = null;
//            for (Triple<String, Field, Function<T, Object>> opt : opts) {
//                propGetter = opt.getRight();
//                if (propGetter != null) {
//                    val = propGetter.apply(instance);
//                } else {
//                    if (StringUtils.isEmpty(opt.getLeft())) {
//                        throw new IllegalStateException("fieldKey can not be empty");
//                    }
//                    property = Objects.requireNonNull(getProps().get(opt.getLeft())
//                            , "key:" + opt.getLeft() + " relevant props can not be null");
//                    val = property.getVal(instance);
//                }
//                if (val == null) {
//                    continue;
//                }
//                cfg.set(opt.getMiddle(), val);
//            }
//            return cfg;
//        }

        public void add(String fieldName, TISDebuziumProp option) {
            this.add(fieldName, option, null);
        }

        public void add(Field option, Function<T, Object> propGetter) {
            this.add(null, TISDebuziumProp.create(option), propGetter);
        }

        public void add(String fieldName, TISDebuziumProp option, Function<T, Object> propGetter) {
            if (StringUtils.isNotEmpty(fieldName)) {
                this.addFieldDescriptor(fieldName, option.configOption, option.overwriteProp);

            }
            this.opts.add(FieldTriple.of(fieldName, option.configOption, propGetter));
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

    public static class TISDebuziumProp {
        private OverwriteProps overwriteProp = new OverwriteProps();
        private final Field configOption;

        boolean hasSetOverWrite = false;

        public static TISDebuziumProp create(Field configOption) {
            return new TISDebuziumProp(configOption);
        }

        private TISDebuziumProp(Field configOption) {
            this.configOption = configOption;
        }

        public TISDebuziumProp overwriteDft(Object dftVal) {
            // overwriteProp.setDftVal(dftVal);
            return this.setOverwriteProp(OverwriteProps.dft(dftVal));
        }

        public TISDebuziumProp overwritePlaceholder(Object placeholder) {
            return this.setOverwriteProp(OverwriteProps.placeholder(placeholder));
        }

        public TISDebuziumProp setOverwriteProp(OverwriteProps overwriteProp) {
            if (hasSetOverWrite) {
                throw new IllegalStateException("overwriteProp has been setted ,can not be writen twice");
            }
            this.overwriteProp = overwriteProp;
            this.hasSetOverWrite = true;
            return this;
        }
    }

    protected void addFieldDescriptor(String fieldName, Field configOption, OverwriteProps overwriteProps) {
        String desc = configOption.description();

        Object dftVal = overwriteProps.processDftVal(configOption.defaultValue());

        StringBuffer helperContent = new StringBuffer(desc);
        if (overwriteProps.appendHelper.isPresent()) {
            helperContent.append("\n\n").append(overwriteProps.appendHelper.get());
        }

        Type targetClazz = configOption.type();
        List<Option> opts = null;
        switch (targetClazz) {
            case LIST: {
                throw new IllegalStateException("unsupported type:" + targetClazz);
            }
            case BOOLEAN: {
                opts = Lists.newArrayList(new Option("是", true), new Option("否", false));
                break;
            }
            case CLASS:
            case PASSWORD:
            case INT:
            case DOUBLE:
            case LONG:
            case SHORT:
            case STRING:
            default:
                // throw new IllegalStateException("unsupported type:" + targetClazz);
        }

        if (configOption.recommender() instanceof EnumRecommender) {
            EnumRecommender enums = (EnumRecommender) configOption.recommender();
            List vals = enums.validValues(null, null);
            opts = (List<Option>) vals.stream().map((e) -> new Option(String.valueOf(e))).collect(Collectors.toList());
        }


//        if (targetClazz == Duration.class) {
//            if (dftVal != null) {
//                dftVal = ((Duration) dftVal).getSeconds();
//            }
//            helperContent.append("\n\n 单位：`秒`");
//        } else if (targetClazz == MemorySize.class) {
//            if (dftVal != null) {
//                dftVal = ((MemorySize) dftVal).getKibiBytes();
//            }
//            helperContent.append("\n\n 单位：`kb`");
//        } else if (targetClazz.isEnum()) {
//            List<Enum> enums = EnumUtils.getEnumList((Class<Enum>) targetClazz);
//            opts = enums.stream().map((e) -> new Option(e.name())).collect(Collectors.toList());
//        } else if (targetClazz == Boolean.class) {
//            opts = Lists.newArrayList(new Option("是", true), new Option("否", false));
//        }

        descriptor.addFieldDescriptor(fieldName, dftVal, configOption.displayName(), helperContent.toString()
                , overwriteProps.opts.isPresent() ? overwriteProps.opts : Optional.ofNullable(opts));
    }


}
