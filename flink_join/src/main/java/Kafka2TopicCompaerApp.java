import com.belle.flinkcdc.compare.func.FlatMapProcessFunction;
import com.belle.flinkcdc.compare.func.JoinProcessFunction;
import com.belle.flinkcdc.compare.func.KeyProcessFunction;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatJoinFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.RichJoinFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.FlatMapIterator;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.calcite.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import java.util.Iterator;

/**
 * @author zhuhaohao
 * @version 1.0
 * @date 2022/11/7/007 22:06
 */
public class Kafka2TopicCompaerApp {

    private static String KAFKASERVER ="szsjhl-damai-mysql-test-10-10-223-19-belle.lan:9092,szsjhl-edcp-mysql-test-10-10-223-20-belle.lan:9092,szsjhl-damai-mysql-test-10-10-223-16-belle.lan:9092";


    // 本次要做的内容为，实现两个kafka中的数据进行比较，通过双流join的方式实现

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setParallelism(1);

        //指定时间进行消费，方便两个数据流的数据尽可能一致
        //时间为15:50
        Long timeStamp = 1667893800000l;

        SingleOutputStreamOperator<JsonNode>  dctSource = env.fromSource(kafkaConsumer("topic_dp_11_dct_format_tmp",timeStamp), WatermarkStrategy.noWatermarks(), "dctSource").flatMap(new FlatMapProcessFunction());
        SingleOutputStreamOperator<JsonNode>  cdcSource = env.fromSource(kafkaConsumer("topic_dp_11_cdc_format_tmp", timeStamp), WatermarkStrategy.noWatermarks(), "cdcSource").flatMap(new FlatMapProcessFunction());

        //做双流join
        DataStream<String> judgeResult = dctSource.join(cdcSource)
                .where(new KeyProcessFunction())
                .equalTo(new KeyProcessFunction())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                      //  .apply(new JoinProcessFunction());
                .apply(getFunction());

        judgeResult.print();



        env.execute();
    }


    private static JoinFunction<JsonNode, JsonNode, String> getFunction() {
        return new JoinFunction<JsonNode, JsonNode, String>() {
            @Override
            public String join(JsonNode dctJson, JsonNode cdcJson) throws Exception {
                JsonNode dctJsonRows = dctJson.get("data").get("rows");
                JsonNode cdcJsonRows = cdcJson.get("data").get("rows");
                return dctJsonRows + "=>" + cdcJsonRows;
            }
        };
    }


    public static KafkaSource<String> kafkaConsumer(String topic,Long timestamp){
        // kafkasource 读取源数据
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKASERVER)
                .setTopics(topic)
                .setGroupId("compare-cdc-dct-format")
                .setStartingOffsets(OffsetsInitializer.timestamp(timestamp))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        return source;
    }

}
