/**
 * Copyright (c) 2020 QingLang, Inc. <baisui@qlangtech.com>
 * <p>
 * This program is free software: you can use, redistribute, and/or modify
 * it under the terms of the GNU Affero General Public License, version 3
 * or later ("AGPL"), as published by the Free Software Foundation.
 * <p>
 * This program is distributed in the hope that it will be useful, but WITHOUT
 * ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
 * FITNESS FOR A PARTICULAR PURPOSE.
 * <p>
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */

package com.qlangtech.tis.plugin.datax;

import com.alibaba.fastjson.JSONObject;
import com.qlangtech.tis.runtime.module.misc.VisualType;
import com.qlangtech.tis.solrdao.ISchemaField;
import com.qlangtech.tis.solrdao.pojo.BasicSchemaField;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-11 16:38
 **/
public class ESField extends BasicSchemaField {

    private VisualType type;

    public VisualType getType() {
        return type;
    }

    public void setType(VisualType type) {
        this.type = type;
    }

    @Override
    public String getTisFieldTypeName() {
        return type.getType();
    }

    @Override
    public boolean isDynamic() {
        return false;
    }

    @Override
    public void serialVisualType2Json(JSONObject f) {
        if (this.getType() == null) {
            throw new IllegalStateException("field:" + this.getName() + " 's fieldType is can not be null");
        }

        VisualType esType = this.getType();
        String type = esType.type;
        EsTokenizerType tokenizerType = EsTokenizerType.parse(this.getTokenizerType());
        if (tokenizerType == null) {
            // 非分词字段
            if (esType.isSplit()) {
                setStringType(f, type, EsTokenizerType.getTokenizerType().name());
            } else {
                f.put("split", false);
                VisualType vtype = EsTokenizerType.visualTypeMap.get(type);
                if (vtype != null) {
                    f.put(ISchemaField.KEY_FIELD_TYPE, vtype.getType());
                    return;
                }
                f.put(ISchemaField.KEY_FIELD_TYPE, type);
            }
        } else {
            // 分词字段
            setStringType(f, tokenizerType.getKey(), EsTokenizerType.getTokenizerType().name());
        }
    }
}
