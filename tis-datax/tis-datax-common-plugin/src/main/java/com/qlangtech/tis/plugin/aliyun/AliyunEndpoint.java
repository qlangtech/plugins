package com.qlangtech.tis.plugin.aliyun;


import com.alibaba.citrus.turbine.Context;
import com.qlangtech.tis.config.aliyun.IAliyunAccessKey;
import com.qlangtech.tis.config.aliyun.IAliyunEndpoint;
import com.qlangtech.tis.extension.Descriptor;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.AuthToken;
import com.qlangtech.tis.plugin.HttpEndpoint;
import com.qlangtech.tis.plugin.IEndTypeGetter;
import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.SocketTimeoutException;
import java.net.URL;
import java.net.UnknownHostException;
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
        protected boolean verify(IControlMsgHandler msgHandler, Context context, PostFormVals postFormVals) {
            AliyunEndpoint endpoint = (AliyunEndpoint) postFormVals.newInstance();

//            IAliyunAccessKey accessKey = null;
//            try {
//                accessKey = endpoint.getAccessKey();
//            } catch (UnsupportedOperationException e) {
//                msgHandler.addErrorMessage(context, "请配置有效的 AccessKey");
//                return false;
//            }
//
//            if (accessKey == null || StringUtils.isBlank(accessKey.getAccessKeyId())
//                    || StringUtils.isBlank(accessKey.getAccessKeySecret())) {
//                msgHandler.addErrorMessage(context, "AccessKeyId 或 AccessKeySecret 不能为空");
//                return false;
//            }

            HttpURLConnection conn = null;
            try {
                URL url = new URL(endpoint.getEndpoint());
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("HEAD");
                conn.setConnectTimeout(5000);
                conn.setReadTimeout(5000);
                conn.setInstanceFollowRedirects(false);

                int responseCode = conn.getResponseCode();
                msgHandler.addActionMessage(context, "阿里云服务端连接验证成功，响应码: " + responseCode);
                return true;
            } catch (UnknownHostException e) {
                msgHandler.addFieldError(context, FIELD_ENDPOINT, "无法解析服务端地址，请检查 Endpoint 配置是否正确: " + e.getMessage());
                return false;
            } catch (SocketTimeoutException e) {
                msgHandler.addFieldError(context, FIELD_ENDPOINT, "连接服务端超时，请检查网络或 Endpoint 配置: " + e.getMessage());
                return false;
            } catch (IOException e) {
                msgHandler.addFieldError(context, FIELD_ENDPOINT, "连接服务端失败: " + e.getMessage());
                return false;
            } finally {
                if (conn != null) {
                    conn.disconnect();
                }
            }
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
