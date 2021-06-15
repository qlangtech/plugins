/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 *   This program is free software: you can use, redistribute, and/or modify
 *   it under the terms of the GNU Affero General Public License, version 3
 *   or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 *  This program is distributed in the hope that it will be useful, but WITHOUT
 *  ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 *   FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.tis.plugin.datax;

import com.alibaba.fastjson.JSONArray;
import com.google.common.collect.Lists;
import com.qlangtech.tis.runtime.module.misc.ISearchEngineTokenizerType;
import com.qlangtech.tis.runtime.module.misc.TokenizerType;
import com.qlangtech.tis.runtime.module.misc.VisualType;
import com.qlangtech.tis.solrdao.ISchema;
import com.qlangtech.tis.solrdao.SolrFieldsParser;

import java.util.List;
import java.util.Map;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-11 16:22
 **/
public class ESSchema implements ISchema {
    private String uniqueKey;
    private String sharedKey;
    public List<ESField> fields = Lists.newArrayList();


    public void setUniqueKey(String uniqueKey) {
        this.uniqueKey = uniqueKey;
    }

    public void setSharedKey(String sharedKey) {
        this.sharedKey = sharedKey;
    }

    public List<ESField> getFields() {
        return this.fields;
    }

    public void setFields(List<ESField> fields) {
        this.fields = fields;
    }

    @Override
    public List<ESField> getSchemaFields() {
        // List<ISchemaField> fields = Lists.newArrayList();
        return fields;
    }

    @Override
    public void clearFields() {
        this.fields.clear();
    }

    @Override
    public String getUniqueKey() {
        return this.uniqueKey;
    }

    @Override
    public String getSharedKey() {
        return this.sharedKey;
    }

    @Override
    public JSONArray serialTypes() {

        JSONArray types = new JSONArray();
        com.alibaba.fastjson.JSONObject f = null;
        JSONArray tokens = null;
        com.alibaba.fastjson.JSONObject tt = null;
        SolrFieldsParser.SolrType solrType = null;
        // Set<String> typesSet = new HashSet<String>();
        for (Map.Entry<String, VisualType> t : EsTokenizerType.visualTypeMap.entrySet()) {
            f = new com.alibaba.fastjson.JSONObject();
            f.put("name", t.getKey());
            f.put("split", t.getValue().isSplit());
            tokens = new JSONArray();
            if (t.getValue().isSplit()) {
                // 默认类型
                for (ISearchEngineTokenizerType tokenType : t.getValue().getTokenerTypes()) {
                    tt = new com.alibaba.fastjson.JSONObject();
                    tt.put("key", tokenType.getKey());
                    tt.put("value", tokenType.getDesc());
                    tokens.add(tt);
                }
                // 外加类型
//                for (Map.Entry<String, SolrFieldsParser.SolrType> entry : this.types.entrySet()) {
//                    solrType = entry.getValue();
//                    if (solrType.tokenizerable) {
//                        tt = new com.alibaba.fastjson.JSONObject();
//                        tt.put("key", entry.getKey());
//                        tt.put("value", entry.getKey());
//                        tokens.add(tt);
//                    }
//                }
                f.put("tokensType", tokens);
            }
            types.add(f);
        }

//        for (Map.Entry<String, SolrFieldsParser.SolrType> entry : this.types.entrySet()) {
//            solrType = entry.getValue();
//            if (!solrType.tokenizerable && !TokenizerType.isContain(entry.getKey())) {
//                f = new com.alibaba.fastjson.JSONObject();
//                f.put("name", entry.getKey());
//                types.add(f);
//            }
//        }
        // schema.put("fieldtypes", types);
        return types;
    }


}
