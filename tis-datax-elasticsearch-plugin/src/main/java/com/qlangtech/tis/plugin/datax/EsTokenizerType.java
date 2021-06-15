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

import com.alibaba.datax.plugin.writer.elasticsearchwriter.ESFieldType;
import com.google.common.collect.ImmutableMap;
import com.qlangtech.tis.runtime.module.misc.ISearchEngineTokenizerType;
import com.qlangtech.tis.runtime.module.misc.VisualType;
import org.apache.commons.lang.StringUtils;

import java.util.Map;

/**
 * @author: 百岁（baisui@qlangtech.com）
 * @create: 2021-06-12 16:43
 **/
public enum EsTokenizerType implements ISearchEngineTokenizerType {

    NULL(StringUtils.lowerCase(ESFieldType.STRING.name()), "无分词"),
    STANDARD("standard", "Standard"),
    SIMPLE("simple", "Simple"),
    WHITESPACE("whitespace", "Whitespace"),
    STOP("stop", "Stop"),
    KEYWORD("keyword", "Keyword"),
    PATTERN("pattern", "Pattern"),
    FINGERPRINT("fingerprint", "Fingerprint");

    public static final Map<String, VisualType> visualTypeMap;

    static {
        ImmutableMap.Builder<String, VisualType> visualTypeMapBuilder = new ImmutableMap.Builder<>();
        String typeName = null;
        VisualType type = null;
        for (ESFieldType t : com.alibaba.datax.plugin.writer.elasticsearchwriter.ESFieldType.values()) {
            if (t == ESFieldType.STRING) {
                // 作为tokener的一个类型
                continue;
            }
            typeName = StringUtils.lowerCase(t.name());
            if (t == getTokenizerType()) {
                type = new VisualType(typeName, true) {
                    @Override
                    public ISearchEngineTokenizerType[] getTokenerTypes() {
                        return EsTokenizerType.values();
                    }
                };
            } else {
                type = new VisualType(typeName, false);
            }
            visualTypeMapBuilder.put(typeName, type);
        }
        visualTypeMap = visualTypeMapBuilder.build();
    }

    public static ESFieldType getTokenizerType() {
        return ESFieldType.TEXT;
    }

    private final String key;
    private final String desc;

    /**
     * @param key
     * @param desc
     */
    private EsTokenizerType(String key, String desc) {
        this.key = key;
        this.desc = desc;
    }

    public static EsTokenizerType parse(String type) {
        for (EsTokenizerType t : EsTokenizerType.values()) {
            if (StringUtils.equals(type, t.key)) {
                return t;
            }
        }
        return null;
    }

    public String getKey() {
        return key;
    }

    public String getDesc() {
        return desc;
    }
}
