package com.belle.flinkcdc.compare.func;

import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.calcite.shaded.com.fasterxml.jackson.databind.JsonNode;
/**
 * @author : zhuhaohao
 * @date :
 */
public class KeyProcessFunction implements KeySelector<JsonNode, String> {

    @Override
    public String getKey(JsonNode dctJson) throws Exception {
        JsonNode header = dctJson.get("header");
        JsonNode location = dctJson.get("location");

        String databaseName = header.get("catalog").asText();
        String tableName = header.get("table").asText();
        String id = location.get("id").asText();

        StringBuilder result = new StringBuilder();
        result.append(databaseName);
        result.append(tableName);
        result.append(id);


        return result.toString();
    }
}
