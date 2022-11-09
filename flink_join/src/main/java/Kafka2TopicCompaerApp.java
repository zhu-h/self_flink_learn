import com.belle.flinkcdc.compare.func.FlatMapProcessFunction;
import com.belle.flinkcdc.compare.func.KeyProcessFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.calcite.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.sql.Timestamp;


/**
 * @author zhuhaohao
 * @version 1.0
 * @date 2022/11/7/007 22:06
 */
@Slf4j
public class Kafka2TopicCompaerApp {

    // 本次要做的内容为，实现两个kafka中的数据进行比较，通过双流join的方式实现
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        ParameterTool params = ParameterTool.fromArgs(args);
        String dctsourcetopic = params.get("dctsourcetopic");
        String cdcsourcetopic = params.get("cdcsourcetopic");
        String dctsourceserver = params.get("dctsourceserver");
        String cdcsourceserver = params.get("cdcsourceserver");
        String starttime = params.get("starttime");
        String stoptime = params.get("stoptime");
        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        //指定时间进行消费，方便两个数据流的数据尽可能一致
        //时间为15:50
        Long starttimeStamp = Timestamp.valueOf(starttime).getTime();
        Long stoptimeStamp =  Timestamp.valueOf(stoptime).getTime();
        SingleOutputStreamOperator<JsonNode>  dctSource = env.fromSource(kafkaConsumer(dctsourcetopic,dctsourceserver,starttimeStamp,stoptimeStamp), WatermarkStrategy.noWatermarks(), "dctSource").flatMap(new FlatMapProcessFunction());
        SingleOutputStreamOperator<JsonNode>  cdcSource = env.fromSource(kafkaConsumer(cdcsourcetopic,cdcsourceserver,starttimeStamp,stoptimeStamp), WatermarkStrategy.noWatermarks(), "cdcSource").flatMap(new FlatMapProcessFunction());
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
                if(dctJsonRows.equals(cdcJsonRows)){
                    log.info("比对正确");
                    return "比对结果正确";
                }else {
                    log.error(dctJsonRows + ">>" + cdcJson);
                    return "!对比ERROR";
                }
            }
        };
    }

    public static KafkaSource<String> kafkaConsumer(String topic,String server,Long startimestamp,Long stopimestamp){
        // kafkasource 读取源数据
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(server)
                .setTopics(topic)
                .setGroupId("compare-cdc-dct-format")
                .setStartingOffsets(OffsetsInitializer.timestamp(startimestamp))
                .setBounded(OffsetsInitializer.timestamp(stopimestamp))
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
        return source;
    }
}
