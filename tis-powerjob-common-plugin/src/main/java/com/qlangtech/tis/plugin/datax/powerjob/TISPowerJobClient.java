package com.qlangtech.tis.plugin.datax.powerjob;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.alibaba.fastjson.parser.Feature;
import com.qlangtech.tis.datax.job.IRegisterApp;
import com.qlangtech.tis.datax.job.PowerjobOrchestrateException;
import com.qlangtech.tis.extension.TISExtension;
import com.qlangtech.tis.plugin.datax.powerjob.impl.TISWorkflowInfoDTO;
import com.qlangtech.tis.plugin.datax.powerjob.impl.WorkflowListResult;
import okhttp3.FormBody;
import okhttp3.MediaType;
import okhttp3.RequestBody;
import org.apache.commons.lang.StringUtils;
import tech.powerjob.client.PowerJobClient;
import tech.powerjob.client.TypeStore;
import tech.powerjob.common.OmsConstant;
import tech.powerjob.common.OpenAPIConstant;
import tech.powerjob.common.exception.PowerJobException;
import tech.powerjob.common.response.ResultDTO;
import tech.powerjob.common.response.WorkflowInfoDTO;
import tech.powerjob.common.utils.HttpUtils;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.Objects;

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
    static final Method assertAppMethod;
    static final Method getUrlMethod;

    static {
        try {
            appIdField = PowerJobClient.class.getDeclaredField("appId");
            appIdField.setAccessible(true);

            //postHA(String path, RequestBody requestBody)

            postHAMethod = PowerJobClient.class.getDeclaredMethod("postHA", String.class, RequestBody.class);
            postHAMethod.setAccessible(true);

            // String appName, String password, String url
            final String assertAppMethodName = "assertApp";
            assertAppMethod = Objects.requireNonNull(PowerJobClient.class.getDeclaredMethod(
                    assertAppMethodName, String.class, String.class, String.class), assertAppMethodName + " relevant method instance can not be null");
            assertAppMethod.setAccessible(true);

            //  private static String getUrl(String path, String address) {
            final String getUrlMethodName = "getUrl";
            getUrlMethod = PowerJobClient.class.getDeclaredMethod(getUrlMethodName, String.class, String.class);
            getUrlMethod.setAccessible(true);

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


    @TISExtension
    public static IRegisterApp registerApp() {

        return new IRegisterApp() {
            private RegisterAppResult assertLogin(String addr, String appName, String password) {


                try {
                    String url = (String) getUrlMethod.invoke(null, "/assert", addr);
                    String result = (String) assertAppMethod.invoke(null, appName, password, url);// assertApp();
                    if (StringUtils.isEmpty(result)) {
                        throw new IllegalStateException("assert valid app request:" + appName + " url:" + url + " relevant result can not be null");
                    }
                    //  if (org.apache.commons.lang3.StringUtils.isNotEmpty(result)) {
                    ResultDTO<Long> resultDTO = (ResultDTO) JSON.parseObject(result, TypeStore.LONG_RESULT_TYPE, new Feature[0]);
                    if (!resultDTO.isSuccess()) {
                        boolean passwordInvalid = false;
                        if (StringUtils.indexOfIgnoreCase(resultDTO.getMessage(), "password error") > -1) {
                            passwordInvalid = true;
                        }
                        // 可能密码错误
                        // throw new PowerJobException(resultDTO.getMessage());
                        return new RegisterAppResult(false, passwordInvalid, resultDTO.getMessage());
                    }

                    return new RegisterAppResult(true, false, resultDTO.getMessage());
//                this.appId = (Long) resultDTO.getData();
//                this.currentAddress = addr;
//                break;
                    //}
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }

            /**
             * 注册新app
             *
             * @param domain   : format: IP:PORT
             * @param appName
             * @param password
             */
            @Override
            public void registerApp(String domain, String appName, String password) throws PowerjobOrchestrateException {
                if (StringUtils.isEmpty(domain)) {
                    throw new IllegalArgumentException("param powerjobDomain can not be empty");
                }
                if (StringUtils.isEmpty(appName)) {
                    throw new IllegalArgumentException("param appName can not be empty");
                }
                if (StringUtils.isEmpty(password)) {
                    throw new IllegalArgumentException("param password can not be empty");
                }
                RegisterAppResult registerResult = assertLogin(domain, appName, password);
                if (!registerResult.isSuccess()) {
                    // 用户名密码错误，或者账户不存在
                    // 如果是密码错误？ 怎么办？
                    if (registerResult.isPasswordInvalid()) {
                        throw new PowerjobOrchestrateException(registerResult.getMessage());
                    }
                    // 创建账户
                    MediaType jsonType = MediaType.parse(OmsConstant.JSON_MEDIA_TYPE);
                    String json = " {\"appName\": \"" + appName + "\", \"password\": \"" + password + "\"}";
                    String post = overWritePostHA(domain, "/appInfo/save", RequestBody.create(jsonType, json));
                    ResultDTO<Object> wfResult = JSON.parseObject(post, REGISTER_APP_RESULT_TYPE);
                    result(wfResult);
                } else {
                    // 用户已经存在了，直接跳出
                }


            }
        };


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
