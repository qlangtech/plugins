//package com.qlangtech.plugins.incr.flink.alert;
//
//import com.alibaba.citrus.turbine.Context;
//import com.alibaba.fastjson.JSONArray;
//import com.alibaba.fastjson.JSONObject;
//import com.qlangtech.tis.extension.IPropertyType;
//import com.qlangtech.tis.plugin.ds.CMeta;
//import com.qlangtech.tis.plugin.ds.ElementCreatorFactory;
//import com.qlangtech.tis.plugin.ds.ViewContent;
//import com.qlangtech.tis.runtime.module.misc.IControlMsgHandler;
//
//import java.util.function.BiConsumer;
//
///**
// *
// * @author 百岁 (baisui@qlangtech.com)
// * @date 2025/11/18
// */
//public class AlterChannelElementCreatorFactory implements ElementCreatorFactory<AlterChannelElement> {
//    @Override
//    public CMeta.ParsePostMCols<AlterChannelElement>
//    parsePostMCols(IPropertyType propertyType, IControlMsgHandler msgHandler
//            , Context context, String keyColsMeta, JSONArray targetCols) {
//        CMeta.ParsePostMCols<AlterChannelElement> cols = new CMeta.ParsePostMCols<>();
//        cols.writerCols = null;
//        cols.pkHasSelected = false;
//        cols.validateFaild = false;
//        return cols;
//    }
//
//    @Override
//    public ViewContent getViewContentType() {
//        return ViewContent.JdbcTypeProps;
//    }
//
//    @Override
//    public AlterChannelElement createDefault(JSONObject targetCol) {
//        return new AlterChannelElement();
//    }
//
//    @Override
//    public AlterChannelElement create(JSONObject targetCol, BiConsumer<String, String> errorProcess) {
//        return new AlterChannelElement();
//    }
//}
