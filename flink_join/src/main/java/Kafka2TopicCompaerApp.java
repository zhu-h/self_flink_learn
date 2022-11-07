import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @author zhuhaohao
 * @version 1.0
 * @date 2022/11/7/007 22:06
 */
public class Kafka2TopicCompaerApp {

    // 本次要做的内容为，实现两个kafka中的数据进行比较，通过双流join的方式实现

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.execute();
    }
}
