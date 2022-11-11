package com.belle;

import org.apache.flink.api.java.utils.ParameterTool;

/**
 * @author : zhuhaohao
 * @date :
 */
public class flinkconfig {
    public static void main(String[] args) {

        // flink read config

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        boolean flink_conf = parameterTool.has("flink_conf");

        System.out.println(flink_conf);
    }
}
