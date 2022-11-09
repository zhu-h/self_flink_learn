package com.belle.flinkcdc.compare.func;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.calcite.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.calcite.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.util.Collector;


/**
 * @author : zhuhaohao
 * @date :
 */
public class DatabaseTableCountFlatFunction extends RichFlatMapFunction<Tuple2<Integer, String>, Tuple3<String,String,Integer>> {

        private ObjectMapper mapper;
        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            mapper = new ObjectMapper();
        }
        @Override
        public void flatMap(Tuple2<Integer, String> value, Collector<Tuple3<String,String,Integer>> out) throws Exception {
            if (value.f0 == 1){
                JsonNode cdcJsonNode = mapper.readTree(value.f1);
                JsonNode header = cdcJsonNode.get("header");
                String catalog = header.get("catalog").asText();
                String table = header.get("table").asText();
                out.collect(new Tuple3<>(catalog,table,1));
            }
        }
    }

