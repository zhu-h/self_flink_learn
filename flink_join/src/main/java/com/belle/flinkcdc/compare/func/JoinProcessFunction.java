package com.belle.flinkcdc.compare.func;

import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;

/**
 * @author : zhuhaohao
 * @date :
 */
public class JoinProcessFunction implements JoinFunction<JsonNode,JsonNode,String> {

    @Override
    public String join(JsonNode first, JsonNode second) throws Exception {
        return null;
    }
}
