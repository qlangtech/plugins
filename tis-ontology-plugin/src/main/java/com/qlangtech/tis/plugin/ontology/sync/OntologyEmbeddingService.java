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
package com.qlangtech.tis.plugin.ontology.sync;

import ai.onnxruntime.OnnxTensor;
import ai.onnxruntime.OrtEnvironment;
import ai.onnxruntime.OrtException;
import ai.onnxruntime.OrtSession;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

/**
 * 基于 ONNX Runtime 的 384 维句向量推理服务（paraphrase-multilingual-MiniLM-L12-v2）。
 * 同时内嵌 BERT WordPiece 分词器，无需额外分词库依赖。
 *
 * <p>模型文件路径：classpath {@code models/model.onnx}，词表：{@code models/vocab.txt}。
 * 文件不存在时 fallback 到随机归一化向量（演示流程不中断）。
 *
 * @author 百岁 (baisui@qlangtech.com)
 * @date 2026/5/28
 */
public class OntologyEmbeddingService implements AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(OntologyEmbeddingService.class);

    public static final int EMBEDDING_DIM = 384;

    private static final String MODEL_RESOURCE = "models/model.onnx";
    private static final String VOCAB_RESOURCE  = "models/vocab.txt";

    // ONNX 模型输入 tensor 名称
    private static final String INPUT_IDS_NAME      = "input_ids";
    private static final String ATTENTION_MASK_NAME  = "attention_mask";
    private static final String TOKEN_TYPE_IDS_NAME  = "token_type_ids";

    // Tokenizer 特殊 token
    private static final int CLS_ID = 101;
    private static final int SEP_ID = 102;
    private static final int UNK_ID = 100;
    private static final int MAX_SEQ = 128;

    private OrtEnvironment ortEnv;
    private OrtSession ortSession;
    private final Map<String, Integer> vocab;
    private final boolean modelLoaded;

    public OntologyEmbeddingService() {
        this.vocab = loadVocab();
        boolean loaded = false;
        try {
            InputStream ms = getClass().getClassLoader().getResourceAsStream(MODEL_RESOURCE);
            if (ms == null) {
                log.warn("[OnnxEmbed] model not found: {}, using random-vector fallback", MODEL_RESOURCE);
            } else {
                Path tmp = Files.createTempFile("ontology-minilm-", ".onnx");
                tmp.toFile().deleteOnExit();
                Files.copy(ms, tmp, StandardCopyOption.REPLACE_EXISTING);
                this.ortEnv = OrtEnvironment.getEnvironment();
                this.ortSession = ortEnv.createSession(tmp.toString(), new OrtSession.SessionOptions());
                loaded = true;
                log.info("[OnnxEmbed] ONNX model loaded, dim={}", EMBEDDING_DIM);
            }
        } catch (Exception e) {
            log.warn("[OnnxEmbed] model load error, fallback to random vectors: {}", e.getMessage());
        }
        this.modelLoaded = loaded;
    }

    /**
     * 对文本推理，返回 L2 归一化 float[384] 向量。
     * 模型未加载时返回随机归一化向量。
     */
    public float[] embed(String text) {
        if (!modelLoaded) return randomEmbedding();
        try {
            long[][] inputIds = new long[1][];
            long[][] attnMask = new long[1][];
            long[][] typeIds  = new long[1][];
            encode(text, inputIds, attnMask, typeIds);

            OnnxTensor t1 = OnnxTensor.createTensor(ortEnv, inputIds);
            OnnxTensor t2 = OnnxTensor.createTensor(ortEnv, attnMask);
            OnnxTensor t3 = OnnxTensor.createTensor(ortEnv, typeIds);
            Map<String, OnnxTensor> inputs = Map.of(
                    INPUT_IDS_NAME, t1, ATTENTION_MASK_NAME, t2, TOKEN_TYPE_IDS_NAME, t3);
            try (OrtSession.Result result = ortSession.run(inputs)) {
                float[][][] hidden = (float[][][]) result.get(0).getValue();
                return l2Normalize(meanPool(hidden[0], attnMask[0]));
            }
        } catch (OrtException e) {
            log.warn("[OnnxEmbed] inference error: {}", e.getMessage());
            return randomEmbedding();
        }
    }

    /** embed(text) 结果转 List&lt;Float&gt;（Neo4j HNSW 索引要求 Float 而非 Double）。 */
    public List<Float> embedAsList(String text) {
        float[] arr = embed(text);
        List<Float> list = new ArrayList<>(arr.length);
        for (float v : arr) list.add(v);
        return list;
    }

    @Override
    public void close() {
        try {
            if (ortSession != null) ortSession.close();
            if (ortEnv    != null) ortEnv.close();
        } catch (OrtException e) {
            log.warn("[OnnxEmbed] close error: {}", e.getMessage());
        }
    }

    // ---- tokenizer ----

    private void encode(String text,
                        long[][] outIds, long[][] outMask, long[][] outTypeIds) {
        List<Integer> ids = new ArrayList<>();
        ids.add(CLS_ID);
        List<String> tokens = tokenize(text);
        int max = MAX_SEQ - 2;
        if (tokens.size() > max) tokens = tokens.subList(0, max);
        for (String t : tokens) ids.add(vocab.getOrDefault(t, UNK_ID));
        ids.add(SEP_ID);

        int len = ids.size();
        outIds[0]     = new long[len];
        outMask[0]    = new long[len];
        outTypeIds[0] = new long[len];
        for (int i = 0; i < len; i++) {
            outIds[0][i]  = ids.get(i);
            outMask[0][i] = 1L;
        }
    }

    private List<String> tokenize(String text) {
        text = insertSpacesAroundCjkAndPunct(text);
        String[] words = text.trim().split("\\s+");
        List<String> tokens = new ArrayList<>();
        for (String w : words) {
            if (!w.isEmpty()) tokens.addAll(wordPiece(w));
        }
        return tokens;
    }

    private String insertSpacesAroundCjkAndPunct(String text) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (isCjk(c) || isPunct(c)) sb.append(' ').append(c).append(' ');
            else sb.append(c);
        }
        return sb.toString();
    }

    private boolean isCjk(char c) {
        return (c >= '一' && c <= '鿿') || (c >= '㐀' && c <= '䶿') || (c >= '豈' && c <= '﫿');
    }

    private boolean isPunct(char c) {
        int cp = c;
        return (cp >= 33 && cp <= 47) || (cp >= 58 && cp <= 64) || (cp >= 91 && cp <= 96) || (cp >= 123 && cp <= 126);
    }

    private List<String> wordPiece(String word) {
        if (vocab.isEmpty()) {
            List<String> chars = new ArrayList<>();
            for (char c : word.toCharArray()) chars.add(String.valueOf(c));
            return chars;
        }
        List<String> sub = new ArrayList<>();
        int start = 0;
        while (start < word.length()) {
            int end = word.length();
            String cur = null;
            while (start < end) {
                String s = word.substring(start, end);
                if (start > 0) s = "##" + s;
                if (vocab.containsKey(s)) { cur = s; break; }
                end--;
            }
            if (cur == null) return List.of("[UNK]");
            sub.add(cur);
            start = end;
        }
        return sub;
    }

    private Map<String, Integer> loadVocab() {
        Map<String, Integer> v = new HashMap<>();
        try (InputStream is = getClass().getClassLoader().getResourceAsStream(VOCAB_RESOURCE)) {
            if (is == null) { log.warn("[Tokenizer] vocab.txt not found, using char-level fallback"); return v; }
            BufferedReader r = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));
            String line; int idx = 0;
            while ((line = r.readLine()) != null) v.put(line.trim(), idx++);
            log.info("[Tokenizer] vocab loaded, size={}", v.size());
        } catch (IOException e) {
            log.warn("[Tokenizer] vocab load error: {}", e.getMessage());
        }
        return v;
    }

    // ---- math ----

    private float[] meanPool(float[][] hidden, long[] mask) {
        int dim = hidden[0].length;
        float[] pooled = new float[dim];
        long cnt = 0;
        for (int i = 0; i < hidden.length; i++) {
            if (mask[i] == 0) continue;
            cnt++;
            for (int d = 0; d < dim; d++) pooled[d] += hidden[i][d];
        }
        if (cnt > 0) for (int d = 0; d < dim; d++) pooled[d] /= cnt;
        return pooled;
    }

    private float[] l2Normalize(float[] vec) {
        double norm = 0.0;
        for (float v : vec) norm += (double) v * v;
        norm = Math.sqrt(norm);
        if (norm < 1e-12) return vec;
        float[] out = new float[vec.length];
        for (int i = 0; i < vec.length; i++) out[i] = (float) (vec[i] / norm);
        return out;
    }

    private float[] randomEmbedding() {
        Random rng = new Random();
        float[] vec = new float[EMBEDDING_DIM];
        for (int i = 0; i < EMBEDDING_DIM; i++) vec[i] = rng.nextFloat() * 2 - 1;
        return l2Normalize(vec);
    }
}