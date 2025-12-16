package com.qlangtech.tis.plugin.aliyun;


import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.AuthToken;
import com.qlangtech.tis.plugin.AuthToken.IAliyunAccessKey;
import com.qlangtech.tis.plugin.HttpEndpoint;
import com.qlangtech.tis.plugin.HttpEndpoint.IAliyunEndpoint;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.stream.Collectors;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2023-07-27 00:12
 **/
public class AliyunEndpoint extends HttpEndpoint implements IAliyunEndpoint {


    public final IAliyunAccessKey getAccessKey() {
        return this.authToken.accept(new AuthToken.Visitor<IAliyunAccessKey>() {
            @Override
            public IAliyunAccessKey visit(IAliyunAccessKey accessKey) {
                return accessKey;
            }
        });
    }

    public static List<? extends Descriptor> filter(List<? extends Descriptor> descs) {
        if (CollectionUtils.isEmpty(descs)) {
            throw new IllegalArgumentException("param descs can not be null");
        }
        return descs.stream().filter((d) -> {
            return AccessKey.KEY_ACCESS.equals(d.getDisplayName());
        }).collect(Collectors.toList());
    }

    public String getEndpointHost() {
        return StringUtils.substringAfter(this.endpoint, "//");
    }

    @TISExtension()
    public static class DefaultDescriptor extends HttpEndpoint.DefaultDescriptor implements IEndTypeGetter {
        public DefaultDescriptor() {
            super(KEY_FIELD_ALIYUN_TOKEN);
        }

        @Override
        public EndType getEndType() {
            return EndType.Aliyun;
        }

        @Override
        public String getDisplayName() {
            return KEY_FIELD_ALIYUN_TOKEN;
        }
    }
}
