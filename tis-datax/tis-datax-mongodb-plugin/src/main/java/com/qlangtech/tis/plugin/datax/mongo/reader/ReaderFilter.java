package com.qlangtech.tis.plugin.datax.mongo.reader;

import com.alibaba.datax.plugin.reader.mongodbreader.MongoDBReader;
import com.qlangtech.tis.extension.Describable;
import org.bson.Document;

/**
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2023/9/8
 * @see MongoDBReader.Task
 */
public class ReaderFilter implements Describable<ReaderFilter> {

    public Document createFilter() {
        return new Document();
    }

}
