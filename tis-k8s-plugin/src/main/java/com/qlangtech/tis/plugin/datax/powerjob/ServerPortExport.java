package com.qlangtech.tis.plugin.datax.powerjob;

import com.qlangtech.tis.extension.Describable;
import com.qlangtech.tis.plugin.annotation.FormField;
import com.qlangtech.tis.plugin.annotation.FormFieldType;
import com.qlangtech.tis.plugin.annotation.Validator;
import io.kubernetes.client.openapi.ApiException;
import io.kubernetes.client.openapi.apis.CoreV1Api;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/12/18
 */
public abstract class ServerPortExport implements Describable<ServerPortExport> {

    @FormField(ordinal = 0, type = FormFieldType.INT_NUMBER, validate = {Validator.require, Validator.integer})
    public Integer serverPort;

    public abstract void exportPort(String nameSpace, CoreV1Api api, String targetPortName) throws ApiException;



    /**
     * TIS 可用的 host:port 访问地址
     *
     * @return
     */
    public abstract String getPowerjobHost();
}
