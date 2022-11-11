package com.belle;

import org.apache.flink.api.java.utils.ParameterTool;

import java.io.IOException;

/**
 * @author : zhuhaohao
 * @date :
 */
public class FlinkConfig2ReadYaml {
    public static void main(String[] args) throws IOException {

        ParameterTool parameterTool = ParameterTool.fromArgs(args);
        String config_path = parameterTool.get("config_path");

        ParameterTool config = ParameterTool.fromPropertiesFile(config_path);



    }
}
