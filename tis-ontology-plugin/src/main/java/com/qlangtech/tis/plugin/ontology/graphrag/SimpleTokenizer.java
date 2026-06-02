/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.qlangtech.tis.plugin.ontology.graphrag;

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * 轻量分词器：ASCII 词按非字母数字切分，中文段按 1/2/3-gram 切片。
 * 用于 Glossary 同义词精确匹配 / 关键词回退，覆盖 ChatBI 短查询场景。
 * 后续可平替为 HanLP / Jieba（仅替换本类实现，调用方不感知）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/1
 */
public final class SimpleTokenizer {

    private static final int CJK_NGRAM_MIN = 1;
    private static final int CJK_NGRAM_MAX = 3;

    private SimpleTokenizer() {
    }

    /**
     * 切词。返回去重保序后的 token 列表。
     */
    public static List<String> tokenize(String text) {
        Set<String> tokens = new LinkedHashSet<>();
        if (text == null || text.isBlank()) return List.copyOf(tokens);

        StringBuilder asciiBuf = new StringBuilder();
        StringBuilder cjkBuf = new StringBuilder();
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (isCjk(c)) {
                flushAscii(asciiBuf, tokens);
                cjkBuf.append(c);
            } else if (Character.isLetterOrDigit(c)) {
                flushCjk(cjkBuf, tokens);
                asciiBuf.append(Character.toLowerCase(c));
            } else {
                flushAscii(asciiBuf, tokens);
                flushCjk(cjkBuf, tokens);
            }
        }
        flushAscii(asciiBuf, tokens);
        flushCjk(cjkBuf, tokens);
        return List.copyOf(tokens);
    }

    private static void flushAscii(StringBuilder buf, Set<String> tokens) {
        if (buf.length() == 0) return;
        tokens.add(buf.toString());
        buf.setLength(0);
    }

    private static void flushCjk(StringBuilder buf, Set<String> tokens) {
        int len = buf.length();
        if (len == 0) return;
        for (int n = CJK_NGRAM_MIN; n <= CJK_NGRAM_MAX; n++) {
            for (int start = 0; start + n <= len; start++) {
                tokens.add(buf.substring(start, start + n));
            }
        }
        buf.setLength(0);
    }

    private static boolean isCjk(char c) {
        Character.UnicodeBlock block = Character.UnicodeBlock.of(c);
        return block == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS
                || block == Character.UnicodeBlock.CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A
                || block == Character.UnicodeBlock.CJK_COMPATIBILITY_IDEOGRAPHS;
    }
}
