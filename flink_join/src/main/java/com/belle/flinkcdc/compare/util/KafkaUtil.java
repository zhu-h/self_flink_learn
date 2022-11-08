package com.belle.flinkcdc.compare.util;

import org.apache.flink.connector.kafka.source.KafkaSource;

/**
 * @author : zhuhaohao
 * @date :
 */
public class KafkaUtil {





    public KafkaSource<String> kafkaConsumer(){
        // kafkasource 读取源数据
        KafkaSource<String> source = KafkaSource.<String>builder()

                .build();
        return source;
    }

}
