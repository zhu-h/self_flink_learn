package com.belle.flinkcdc.compare.func;

import cn.hutool.core.util.ObjectUtil;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.calcite.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.calcite.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;

/**
 * @author : zhuhaohao
 * @date :
 */
public class FlatMapProcessFunction extends RichFlatMapFunction<String, JsonNode> {
    private static ObjectMapper mapper;
    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        mapper = new ObjectMapper();
    }

    @Override
    public void flatMap(String dctString, Collector<JsonNode> out) throws Exception {
        JsonNode dctJsonPre = mapper.readTree(dctString);
        String operation = dctJsonPre.get("operation").asText();
        if (!operation.equals("delete") && !operation.equals("update") ){
            JsonNode location = dctJsonPre.get("location");
            JsonNode id = location.get("id");
            if (id != null){
                out.collect(dctJsonPre);
            }
        }
    }
}
