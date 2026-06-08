package com.qlangtech.tis.plugin.ontology;

import com.alibaba.fastjson.JSONObject;
import org.junit.Test;

import java.io.IOException;

public class TestStreamingJsonOntologyParserDebug {

    @Test
    public void testSimple() throws IOException {
        String json = "{\"linkTypes\":[{\"name\":\"test\"}]}";

        System.out.println("Input JSON: " + json);
        System.out.println("Length: " + json.length());

        StreamingJsonOntologyParser parser = new StreamingJsonOntologyParser();
        parser.setCallbacks(new StreamingJsonOntologyParser.Callbacks() {
            @Override
            public void onLinkType(JSONObject element) {
                System.out.println("CALLBACK: LinkType = " + element.toJSONString());
            }

            @Override
            public void onSharedProperty(JSONObject element) {
                System.out.println("CALLBACK: SharedProperty = " + element.toJSONString());
            }

            @Override
            public void onValueType(JSONObject element) {
                System.out.println("CALLBACK: ValueType = " + element.toJSONString());
            }

            @Override
            public void onGlossary(JSONObject element) {
                System.out.println("CALLBACK: Glossary = " + element.toJSONString());
            }
        });

        parser.appendChunk(json);
        parser.parse();
        parser.finish();

        System.out.println("Test completed");
    }
}
