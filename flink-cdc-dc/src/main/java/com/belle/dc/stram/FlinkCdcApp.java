package com.belle.dc.stram;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import java.util.Properties;

/**
 * @author : zhuhaohao
 * @date :
 */

// 测试flink cdc2.3.0 对多库多表和指定时间戳的支持问题
public class FlinkCdcApp {
    public static String HOST = "localhost";
    public static int PORT = 3306 ;
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname(HOST)
                .port(PORT)
                .databaseList("flink_cdc_timestamp","magic_api") // set captured database, If you need to synchronize the whole database, Please set tableList to ".*".
                .tableList("flink_cdc_timestamp.table_flink_cdc_test1_default_problem","magic_api.magic_api_file_v2") // set captured table
                .username("root")
                .password("root")
                .startupOptions(StartupOptions.timestamp(1667232000000l))
                .debeziumProperties(getDebeziumProperties())
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();
        env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source")
                .setParallelism(4)
                .print().setParallelism(1); // use parallelism 1 for sink to keep message ordering
        env.execute();
    }
    private static Properties getDebeziumProperties(){
        Properties properties = new Properties();
        properties.setProperty("converters", "dateConverters");
        //根据类在那个包下面修改
        properties.setProperty("dateConverters.type", "com.belle.dc.stram.MySqlDateTimeConverter");
        properties.setProperty("dateConverters.format.date", "yyyy-MM-dd");
        properties.setProperty("dateConverters.format.time", "HH:mm:ss");
        properties.setProperty("dateConverters.format.datetime", "yyyy-MM-dd HH:mm:ss");
        properties.setProperty("dateConverters.format.timestamp", "yyyy-MM-dd HH:mm:ss");
        properties.setProperty("dateConverters.format.timestamp.zone", "UTC+8");
        properties.setProperty("debezium.snapshot.locking.mode","none"); //全局读写锁，可能会影响在线业务，跳过锁设置
        properties.setProperty("include.schema.changes", "true");
        properties.setProperty("bigint.unsigned.handling.mode","long");
        properties.setProperty("decimal.handling.mode","double");
        return properties;
    }
}
