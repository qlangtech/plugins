package com.qlangtech.tis.plugin.datax.powerjob;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.qlangtech.tis.plugin.datax.powerjob.impl.TISWorkflowInfoDTO;
import com.qlangtech.tis.plugin.datax.powerjob.impl.WorkflowListResult;
import okhttp3.FormBody;
import okhttp3.MediaType;
import okhttp3.RequestBody;
import tech.powerjob.client.PowerJobClient;
import tech.powerjob.common.OmsConstant;
import tech.powerjob.common.OpenAPIConstant;
import tech.powerjob.common.exception.PowerJobException;
import tech.powerjob.common.response.ResultDTO;
import tech.powerjob.common.response.WorkflowInfoDTO;
import tech.powerjob.common.utils.HttpUtils;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/11/27
 */
public class TISPowerJobClient extends PowerJobClient {
    public static final TypeReference<ResultDTO<WorkflowListResult>> WF_LIST_RESULT_TYPE = new TypeReference<ResultDTO<WorkflowListResult>>() {
    };
    public static final TypeReference<ResultDTO<TISWorkflowInfoDTO>> WF_TIS_RESULT_TYPE = new TypeReference<ResultDTO<TISWorkflowInfoDTO>>() {
    };

    public static final TypeReference<ResultDTO<Object>> REGISTER_APP_RESULT_TYPE = new TypeReference<ResultDTO<Object>>() {
    };

    static final Field appIdField;
    static final Method postHAMethod;

    static {
        try {
            appIdField = PowerJobClient.class.getDeclaredField("appId");
            appIdField.setAccessible(true);

            //postHA(String path, RequestBody requestBody)

            postHAMethod = PowerJobClient.class.getDeclaredMethod("postHA", String.class, RequestBody.class);
            postHAMethod.setAccessible(true);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private final String domain;
    private final Object appId;

    public TISPowerJobClient(String domain, String appName, String password) {
        super(domain, appName, password);
        this.domain = domain;

        try {
            appId = appIdField.get(this);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Query Workflow by workflowId
     *
     * @param workflowId workflowId
     * @return Workflow meta info
     */
    @Override
    public ResultDTO<WorkflowInfoDTO> fetchWorkflow(Long workflowId) {
        RequestBody body = new FormBody.Builder()
                .add("workflowId", workflowId.toString())
                .add("appId", appId.toString())
                .build();

        try {
            String post = (String) postHAMethod.invoke(this, OpenAPIConstant.FETCH_WORKFLOW, body);
            //  String post = postHA(OpenAPIConstant.FETCH_WORKFLOW, body);
            return JSON.parseObject(post, WF_TIS_RESULT_TYPE);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }


    public static <T> T result(ResultDTO<T> result) {
        if (!result.isSuccess()) {
            throw new IllegalStateException("execute falid:" + result.getMessage());
        }
        return result.getData();
    }

    /**
     * 注册新app
     *
     * @param domain
     * @param appName
     * @param password
     */
    public static void registerApp(String domain, String appName, String password) {
        MediaType jsonType = MediaType.parse(OmsConstant.JSON_MEDIA_TYPE);
        String json = " {\"appName\": \"" + appName + "\", \"password\": \"" + password + "\"}";


//        RequestBody body = new FormBody.Builder().
////                .add("wfInstanceId", wfInstanceId.toString())
////                .add("appId", appId.toString())
//                .build();
        String post = overWritePostHA(domain, "/appInfo/save", RequestBody.create(jsonType, json));
        ResultDTO<Object> wfResult = JSON.parseObject(post, REGISTER_APP_RESULT_TYPE);
        result(wfResult);
    }

    /**
     * @param pageIndex 第一页为0
     * @param pageSize
     * @return
     */
    public WorkflowListResult fetchAllWorkflow(int pageIndex, int pageSize) {

        MediaType jsonType = MediaType.parse(OmsConstant.JSON_MEDIA_TYPE);
        String json = "{\"appId\": " + appId + ", \"index\":" + pageIndex + " ,\"pageSize\":" + pageSize + "}";// JSON.toJSONString();


//        RequestBody body = new FormBody.Builder().
////                .add("wfInstanceId", wfInstanceId.toString())
////                .add("appId", appId.toString())
//                .build();
        String post = overWritePostHA(this.domain, "/workflow/list", RequestBody.create(jsonType, json));
        ResultDTO<WorkflowListResult> wfResult = JSON.parseObject(post, WF_LIST_RESULT_TYPE);
        WorkflowListResult wfList = result(wfResult);
        return wfList;
    }

    private static final String URL_PATTERN = "http://%s%s";

    private static String getUrl(String path, String address) {
        return String.format(URL_PATTERN, address, path);
    }

    private static String overWritePostHA(String domain, String path, RequestBody requestBody) {

        // 先尝试默认地址
        String url = getUrl(path, domain);

        try {
            String res = HttpUtils.post(url, requestBody);

            return res;
        } catch (IOException e) {
            //  throw new RuntimeException(e);
            throw new PowerJobException("no server available when send post request", e);
        }


    }
}
