package com.qlangtech.tis.plugins.incr.flink.cdc;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.util.AbstractPropAssist;
import com.qlangtech.tis.extension.util.PluginExtraProps;
import com.qlangtech.tis.manage.common.Option;
import org.apache.commons.lang3.EnumUtils;
import org.apache.flink.cdc.common.configuration.ConfigOption;
import org.apache.flink.cdc.common.configuration.description.HtmlFormatter;

import java.lang.reflect.Method;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2025-06-19 06:14
 **/
public class FlinkCDCPropAssist<T extends Describable> extends AbstractPropAssist<T,
        org.apache.flink.cdc.common.configuration.ConfigOption> {

    private static final Method getClazzMethod;

    static {
        try {
            getClazzMethod = ConfigOption.class.getDeclaredMethod("getClazz");
            getClazzMethod.setAccessible(true);
        } catch (NoSuchMethodException e) {
            throw new RuntimeException(e);
        }
    }

    public static <PLUGIN extends Describable> Options<PLUGIN,
            org.apache.flink.cdc.common.configuration.ConfigOption> createOpts(Descriptor<PLUGIN> descriptor) {
        FlinkCDCPropAssist props = new FlinkCDCPropAssist(descriptor);
        return props.createOptions();
    }


    static final HtmlFormatter formatter = new HtmlFormatter();

    public FlinkCDCPropAssist(Descriptor<T> descriptor) {
        super(descriptor);
    }

    @Override
    protected MarkdownHelperContent getDescription(ConfigOption configOption) {
        return new MarkdownHelperContent(PluginExtraProps.AsynPropHelp.create(formatter.format(configOption.description())));
    }

    @Override
    protected Object getDefaultValue(ConfigOption configOption) {
        return configOption.defaultValue();
    }

    @Override
    protected List<Option> getOptEnums(ConfigOption configOption) {

        try {
            Class clazz = (Class) getClazzMethod.invoke(configOption);
            if (clazz.isEnum()) {
                Map enumMap = EnumUtils.getEnumMap(clazz);
                Stream<Option> stream = enumMap.keySet().stream().map((key) -> new Option((String) key));
                return stream.collect(Collectors.toList());
            }

        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        return null;
    }

    @Override
    protected String getDisplayName(ConfigOption configOption) {
        return configOption.key();
    }
}
