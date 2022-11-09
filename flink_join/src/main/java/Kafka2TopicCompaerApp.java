import com.belle.flinkcdc.compare.func.DatabaseTableCountFlatFunction;
import com.belle.flinkcdc.compare.func.FlatMapProcessFunction;
import com.belle.flinkcdc.compare.func.KeyProcessFunction;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.JoinFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.calcite.shaded.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.calcite.shaded.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;
import org.apache.kafka.clients.producer.ProducerConfig;


import java.sql.Timestamp;
import java.util.Properties;


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
        String outputtopic = params.get("outputtopic");
        String dctsourceserver = params.get("dctsourceserver");
        String cdcsourceserver = params.get("cdcsourceserver");
        String starttime = params.get("starttime");
        String stoptime = params.get("stoptime");
        String outputerrortopic = params.get("outputerror");


        env.setParallelism(1);
        env.setRuntimeMode(RuntimeExecutionMode.BATCH);
        //指定时间进行消费，方便两个数据流的数据尽可能一致
        //时间为15:50
        Long starttimeStamp = Timestamp.valueOf(starttime).getTime();
        Long stoptimeStamp =  Timestamp.valueOf(stoptime).getTime();
        SingleOutputStreamOperator<JsonNode>  dctSource = env.fromSource(kafkaConsumer(dctsourcetopic,dctsourceserver,starttimeStamp,stoptimeStamp), WatermarkStrategy.noWatermarks(), "dctSource").flatMap(new FlatMapProcessFunction());
        SingleOutputStreamOperator<JsonNode>  cdcSource = env.fromSource(kafkaConsumer(cdcsourcetopic,cdcsourceserver,starttimeStamp,stoptimeStamp), WatermarkStrategy.noWatermarks(), "cdcSource").flatMap(new FlatMapProcessFunction());
        //做双流join
        DataStream<Tuple2<Integer,String>> judgeResult = dctSource.join(cdcSource)
                .where(new KeyProcessFunction())
                .equalTo(new KeyProcessFunction())
                .window(TumblingProcessingTimeWindows.of(Time.seconds(10)))
                .apply(getFunction());

        SingleOutputStreamOperator<Tuple3<String, String, Integer>> dtNum = judgeResult.flatMap(new DatabaseTableCountFlatFunction());
        KeyedStream<Tuple3<String, String, Integer>, Tuple2<String, String>> tuple3Tuple2KeyedStream = dtNum.keyBy(new KeySelector<Tuple3<String, String, Integer>, Tuple2<String, String>>() {
            @Override
            public Tuple2<String, String> getKey(Tuple3<String, String, Integer> value) throws Exception {
                return Tuple2.of(value.f0, value.f1);
            }
        });


        //得到结果输出到kafka中
//        tuple3Tuple2KeyedStream.sum(2).map(r->r.toString()).sinkTo(kafkaProducer(outputtopic,cdcsourceserver));
        tuple3Tuple2KeyedStream.sum(2).map(r->r.toString()).print();


        //judgeResult.filter(r->r.f0==2).map(r->r.toString()).sinkTo(kafkaProducer(outputerrortopic,cdcsourceserver));


        env.execute();
    }

    private static JoinFunction<JsonNode, JsonNode, Tuple2<Integer,String>> getFunction() {
        return new JoinFunction<JsonNode, JsonNode, Tuple2<Integer,String>>() {
            @Override
            public Tuple2<Integer,String> join(JsonNode dctJson, JsonNode cdcJson) throws Exception {
                JsonNode dctJsonRows = dctJson.get("data").get("rows");
                JsonNode cdcJsonRows = cdcJson.get("data").get("rows");
                if(dctJsonRows.equals(cdcJsonRows)){
                    return new Tuple2<>(1,cdcJson+"");
                }else {
                    return new Tuple2<>(2,"!对比ERROR"+ dctJson +">>>"+ cdcJson);
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

    public static KafkaSink<String> kafkaProducer(String topic,String brokersServer){
        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(brokersServer)
                .setRecordSerializer(KafkaRecordSerializationSchema.builder()
                        .setTopic(topic)
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                )
                .setKafkaProducerConfig(getKafkaProducerProps())
                .build();

        return sink;
    }
    private static Properties getKafkaProducerProps() {
        Properties kafkaProducerConfig = new Properties();

        kafkaProducerConfig = new Properties();
        kafkaProducerConfig.put(ProducerConfig.ACKS_CONFIG,"all");
        kafkaProducerConfig.put(ProducerConfig.RETRIES_CONFIG,100);
        kafkaProducerConfig.put(ProducerConfig.RETRY_BACKOFF_MS_CONFIG,10000);
        kafkaProducerConfig.put(ProducerConfig.RECONNECT_BACKOFF_MS_CONFIG,15000);
            kafkaProducerConfig.put(ProducerConfig.RECONNECT_BACKOFF_MAX_MS_CONFIG,300000);
            kafkaProducerConfig.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG,60000);
            kafkaProducerConfig.put(ProducerConfig.MAX_BLOCK_MS_CONFIG,300000);
            kafkaProducerConfig.put(ProducerConfig.BATCH_SIZE_CONFIG,163840);
            kafkaProducerConfig.put(ProducerConfig.LINGER_MS_CONFIG,50);
            kafkaProducerConfig.put(ProducerConfig.BUFFER_MEMORY_CONFIG,67108864);

        return kafkaProducerConfig;
    }

}
