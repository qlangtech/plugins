package com.qlangtech.tis.datax.logger;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.Layout;
import ch.qos.logback.core.UnsynchronizedAppenderBase;
import com.qlangtech.tis.job.common.JobParams;
import com.qlangtech.tis.realtime.utils.NetUtils;
import com.qlangtech.tis.rpc.grpc.log.appender.LoggingEvent;
import com.tis.hadoop.rpc.ITISRpcService;
import com.tis.hadoop.rpc.StatusRpcClientFactory;
import com.tis.hadoop.rpc.StatusRpcClientFactory.AssembleSvcCompsite;
import org.apache.commons.lang3.StringUtils;

import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/11/17
 */
public class TISGrpcAppender extends UnsynchronizedAppenderBase<ILoggingEvent> {
    private String type;
    private String application;
    private String hostname;
    private String logtype;

    protected Layout<ILoggingEvent> layout;


    @Override
    public void start() {
        if (StringUtils.isEmpty(this.logtype)) {
            throw new IllegalArgumentException("property logtype can not be null");
        }
        super.start();
    }

    public void setType(String type) {
        this.type = type;
    }

    public void setApplication(String application) {
        this.application = application;
    }

    public void setLogtype(String logtype) {
        this.logtype = logtype;
    }

    public void setLayout(Layout<ILoggingEvent> layout) {
        this.layout = layout;
    }

    @Override
    protected void append(ILoggingEvent eventObject) {
//        if(DataxExecutor.statusRpc == null){
//
//        }
        ITISRpcService rpcService = AssembleSvcCompsite.statusRpc.get();
        if (rpcService == null) {
            addError("have not initialize rpcService", new Exception());
            return;
        }
        StatusRpcClientFactory.AssembleSvcCompsite svc = rpcService.unwrap();
        try {
            String body = layout != null ? layout.doLayout(eventObject) : eventObject.getFormattedMessage();
            Map<String, String> headers = createHeaders();

            headers.putAll(extractHeaders(eventObject));
            LoggingEvent.Builder eventBuilder = LoggingEvent.newBuilder();
            Level level = eventObject.getLevel();

            eventBuilder.putAllHeaders(headers);
            eventBuilder.setBody(body);
            eventBuilder.setLevel(convertLevel(level));


            svc.append(eventBuilder.build());
        } catch (Exception e) {
            addError(e.getLocalizedMessage(), e);
        }
    }

    private LoggingEvent.Level convertLevel(Level level) {
        if (level.isGreaterOrEqual(Level.ERROR)) {
            return LoggingEvent.Level.ERROR;
        }
        if (level.isGreaterOrEqual(Level.WARN)) {
            return LoggingEvent.Level.WARNING;
        }
        return LoggingEvent.Level.INFO;
    }

    protected HashMap<String, String> createHeaders() {
        return new HashMap<String, String>();
    }

    protected Map<String, String> extractHeaders(ILoggingEvent eventObject) {
        Map<String, String> headers = new HashMap<String, String>(10);
        headers.put("timestamp", Long.toString(eventObject.getTimeStamp()));
        headers.put("type", eventObject.getLevel().toString());
        headers.put("logger", eventObject.getLoggerName());
        // headers.put("message", eventObject.getMessage());
        // headers.put("level", eventObject.getLevel().toString());
        try {
            headers.put("host", resolveHostname());
        } catch (UnknownHostException e) {
            addWarn(e.getMessage());
        }
        headers.put("thread", eventObject.getThreadName());
        if (StringUtils.isNotEmpty(application)) {
            headers.put("application", application);
        }

        if (StringUtils.isNotEmpty(type)) {
            headers.put("type", type);
        }

        headers.put(JobParams.KEY_LOG_TYPE, this.logtype);

        Map<String, String> mdc = eventObject.getMDCPropertyMap();
        String taskId = mdc.get(JobParams.KEY_TASK_ID);
        String collection = StringUtils.defaultIfEmpty(mdc.get(JobParams.KEY_COLLECTION), "unknown");
        headers.put(JobParams.KEY_COLLECTION, collection);
        if (taskId != null) {
            headers.put(JobParams.KEY_TASK_ID, taskId);
        }

        return headers;
    }

    protected String resolveHostname() throws UnknownHostException {
        if (hostname == null) {
            hostname = NetUtils.getHost();
        }
        return hostname;
        // return hostname != null ? hostname : InetAddress.getLocalHost().getHostName();
    }
}
