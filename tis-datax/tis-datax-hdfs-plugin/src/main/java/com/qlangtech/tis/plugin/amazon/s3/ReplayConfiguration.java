package com.qlangtech.tis.plugin.amazon.s3;

import com.google.common.collect.Lists;
import com.qlangtech.tis.manage.common.Option;
import com.qlangtech.tis.offline.FileSystemFactory;
import org.apache.hadoop.conf.Configuration;

import java.util.List;
import java.util.function.BiConsumer;

/**
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/3/10
 */
public class ReplayConfiguration extends Configuration implements FileSystemFactory.IReplayConfiguration {
    private final List<Option> opts = Lists.newArrayList();

    @Override
    public void set(String name, String value) {
        super.set(name, value);
        opts.add(new Option(name, value));
    }

    @Override
    public void replay(BiConsumer<String, String> consumer) {
        opts.forEach((opt) -> consumer.accept(opt.getName(), (String) opt.getValue()));
    }
}
