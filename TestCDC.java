package example;

import com.alibaba.fastjson.JSONObject;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;

import java.util.Objects;
import java.util.Properties;

public class TestCDC {

    public static void main(String[] args) throws Exception {
        //TODO 1 创建流处理环境，设置并行度
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(1);
        // 创建 Properties 对象
        Properties properties = new Properties();
        // 设置时区为 "Asia/Shanghai"
        properties.setProperty("database.serverTimezone", "Asia/Shanghai");
        //TODO 2 创建Flink-MySQL-CDC数据源
        MySqlSource<String> mySqlSource = MySqlSource.<String>builder()
                .hostname("192.168.126.56")
                .port(63307)
                .username("goodcang_dev")
                .password("XXX")
                .databaseList("gc_bsc_common_dev") //设置要捕获的库
                .tableList("gc_bsc_common_dev.fps_overdue_bill") //设置要捕获的表(库不能省略)
                .deserializer(new JsonDebeziumDeserializationSchema()) //将接收到的SourceRecord反序列化为JSON字符串
                .debeziumProperties(properties)
                .startupOptions(StartupOptions.initial()) //启动策略:监视的数据库表执行初始快照，并继续读取最新的binlog
                .build();
        DataStreamSource<String> dataStreamSource = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "mySqlSource");
        dataStreamSource.keyBy(String::length).map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                System.out.println("value ======={}"+value);
                return value;
            }
        }).setParallelism(2);


        // TODO 3 写入Kafka
        KafkaSink<String> kafkaSink = KafkaSink.<String>builder()
                // 指定 kafka 的地址和端口
                .setBootstrapServers("192.168.56.102:9092")
                // 指定序列化器：指定Topic名称、具体的序列化(产生方需要序列化，接收方需要反序列化)
                .setRecordSerializer(KafkaRecordSerializationSchema
                        .<String>builder()
                        .setTopic("test")
                        // 指定value的序列化器
                        .setValueSerializationSchema(new SimpleStringSchema())
                        .build()
                ).build();

        dataStreamSource
                .map(JSONObject::parseObject).map(Objects::toString)
                .sinkTo(kafkaSink);
        env.execute("mysql2kafka");


        //print(env, mySqlSource);

    }

    private static void print(StreamExecutionEnvironment env, MySqlSource<String> mySqlSource) throws Exception {
        // 打印数据
        DataStream<String> result = env.fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "MySQL Source");
        System.out.println(result.toString());
        result.map(new MapFunction<String,String>() {

            @Override
            public String map(String value) throws Exception {
                System.out.println("value ======={}"+value);
                return value;
            }

        });
        env.execute("print2");
    }

}
