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

package com.qlangtech.tis.plugin.ontology;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Objects;

import static com.qlangtech.tis.plugin.ontology.impl.infer.InferOntologyFromLLMStep1.KEY_GLOSSARIES;
import static com.qlangtech.tis.plugin.ontology.impl.infer.InferOntologyFromLLMStep1.KEY_LINK_TYPES;
import static com.qlangtech.tis.plugin.ontology.impl.infer.InferOntologyFromLLMStep1.KEY_SHARED_PROPERTIES;
import static com.qlangtech.tis.plugin.ontology.impl.infer.InferOntologyFromLLMStep1.KEY_VALUE_TYPES;

/**
 * 流式 JSON 解析器，用于增量反序列化 LLM 返回的本体推断结果
 * <p>
 * 使用简单的状态机和深度跟踪来检测完整的 JSON 对象
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/6/7
 */
public class StreamingJsonOntologyParser {

//    private static final String KEY_LINK_TYPES = "linkTypes";
//    private static final String KEY_SHARED_PROPERTIES = "sharedProperties";
//    private static final String KEY_VALUE_TYPES = "valueTypes";
//    private static final String KEY_GLOSSARIES = "glossaries";

    final StringBuilder buffer = new StringBuilder();
    private Callbacks callbacks;

    private ParsingState state = ParsingState.INIT;
    private CurrentArray currentArray = CurrentArray.NONE;
    private int depth = 0;
    private StringBuilder elementBuilder = null;
    private boolean inString = false;
    private boolean escapeNext = false;
    private int processedUpTo = 0; // Track how much of the buffer we've processed

    public interface Callbacks {
        void onLinkType(JSONObject element);

        void onSharedProperty(JSONObject element);

        void onValueType(JSONObject element);

        void onGlossary(JSONObject element);
    }

    private enum ParsingState {
        INIT,
        SEEK_FIELD,
        IN_ARRAY,
        CAPTURING
    }

    private enum CurrentArray {
        NONE,
        LINK_TYPES,
        SHARED_PROPERTIES,
        VALUE_TYPES,
        GLOSSARIES
    }

    public void setCallbacks(Callbacks callbacks) {
        this.callbacks = Objects.requireNonNull(callbacks, "callbacks cannot be null");
    }

    public void appendChunk(String chunk) {
        if (StringUtils.isEmpty(chunk)) {
            return;
        }
        buffer.append(chunk);
    }

    public void parse() throws IOException {
        if (callbacks == null) {
            throw new IllegalStateException("Callbacks must be set before parsing");
        }

        String json = buffer.toString();

        for (int i = processedUpTo; i < json.length(); i++) {
            char c = json.charAt(i);

            // Handle string escaping
            if (state == ParsingState.CAPTURING && elementBuilder != null) {
                if (escapeNext) {
                    elementBuilder.append(c);
                    escapeNext = false;
                    processedUpTo = i + 1;
                    continue;
                }

                if (c == '\\') {
                    elementBuilder.append(c);
                    escapeNext = true;
                    processedUpTo = i + 1;
                    continue;
                }

                if (c == '"') {
                    elementBuilder.append(c);
                    inString = !inString;
                    processedUpTo = i + 1;
                    continue;
                }

                if (inString) {
                    elementBuilder.append(c);
                    processedUpTo = i + 1;
                    continue;
                }
            }

            // Skip whitespace in non-capturing states
            if (state != ParsingState.CAPTURING && Character.isWhitespace(c)) {
                processedUpTo = i + 1;
                continue;
            }

            switch (state) {
                case INIT:
                    if (c == '{') {
                        state = ParsingState.SEEK_FIELD;
                    }
                    processedUpTo = i + 1;
                    break;

                case SEEK_FIELD:
                    if (c == '"') {
                        // Look ahead for closing quote to confirm we have the full field name in buffer.
                        // If not, return and wait for more data — do NOT advance processedUpTo past the
                        // opening quote, otherwise the field name match will be permanently lost when
                        // a chunk boundary splits the field name (e.g. buffer ends at "link before
                        // "linkTypes" is fully received).
                        int closingQuote = -1;
                        for (int j = i + 1; j < json.length(); j++) {
                            char cj = json.charAt(j);
                            if (cj == '\\' && j + 1 < json.length()) {
                                j++; // skip escaped char
                                continue;
                            }
                            if (cj == '"') {
                                closingQuote = j;
                                break;
                            }
                        }
                        if (closingQuote == -1) {
                            // Field name incomplete — wait for next chunk
                            return;
                        }

                        String fieldName = json.substring(i + 1, closingQuote);
                        if (KEY_LINK_TYPES.equals(fieldName)) {
                            currentArray = CurrentArray.LINK_TYPES;
                        } else if (KEY_SHARED_PROPERTIES.equals(fieldName)) {
                            currentArray = CurrentArray.SHARED_PROPERTIES;
                        } else if (KEY_VALUE_TYPES.equals(fieldName)) {
                            currentArray = CurrentArray.VALUE_TYPES;
                        } else if (KEY_GLOSSARIES.equals(fieldName)) {
                            currentArray = CurrentArray.GLOSSARIES;
                        }
                        // unknown field: just skip past the closing quote and keep currentArray as-is
                        i = closingQuote;
                        processedUpTo = i + 1;
                    } else if (c == '[' && currentArray != CurrentArray.NONE) {
                        state = ParsingState.IN_ARRAY;
                        processedUpTo = i + 1;
                    } else {
                        processedUpTo = i + 1;
                    }
                    break;

                case IN_ARRAY:
                    if (c == '{') {
                        state = ParsingState.CAPTURING;
                        elementBuilder = new StringBuilder();
                        elementBuilder.append(c);
                        depth = 1;
                        inString = false;
                        escapeNext = false;
                        processedUpTo = i + 1;
                    } else if (c == ']') {
                        state = ParsingState.SEEK_FIELD;
                        currentArray = CurrentArray.NONE;
                        processedUpTo = i + 1;
                    } else {
                        processedUpTo = i + 1;
                    }
                    break;

                case CAPTURING:
                    elementBuilder.append(c);
                    processedUpTo = i + 1;

                    if (c == '{') {
                        depth++;
                    } else if (c == '}') {
                        depth--;
                        if (depth == 0) {
                            // Complete element captured
                            String elementJson = elementBuilder.toString();
                            try {
                                JSONObject element = JSONObject.parseObject(elementJson);
                                fireCallback(currentArray, element);
                            } catch (Exception e) {
                                System.err.println("Failed to parse element: " + elementJson);
                                throw e;
                            }
                            elementBuilder = null;
                            state = ParsingState.IN_ARRAY;
                        }
                    }
                    break;
            }
        }
    }

    public void finish() throws IOException {
        // Final parse to catch any remaining data
        parse();
    }

    private static final PrintStream print;

    static {
        try {
            print = new PrintStream(FileUtils.openOutputStream(new File("/Users/mozhenghua/j2ee_solution/project/plugins/tis-ontology-plugin/infer.json")));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void fireCallback(CurrentArray arrayType, JSONObject element) {
        print.println(element.toJSONString());
        print.flush();
        switch (arrayType) {
            case LINK_TYPES:
                callbacks.onLinkType(element);
                break;
            case SHARED_PROPERTIES:
                callbacks.onSharedProperty(element);
                break;
            case VALUE_TYPES:
                callbacks.onValueType(element);
                break;
            case GLOSSARIES:
                callbacks.onGlossary(element);
                break;
            default:
                // Should not happen
                break;
        }
    }
}
