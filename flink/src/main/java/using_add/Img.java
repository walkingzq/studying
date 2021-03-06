package using_add;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import partitioner.StringPartitioner;

import java.util.Properties;

/**
 * Created by Zhao Qing on 2018/5/15.
 */
public class Img {
    public static void main(String[] args) throws Exception{

        Img img = new Img();
        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.enableCheckpointing(1000);

        //输入kafka信息（kafka010版本）
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "10.85.115.98:9092,10.85.115.99:9092,10.85.115.100:9092,10.85.115.101:9092,10.85.115.102:9092,10.85.115.103:9092,10.85.115.104:9092,10.85.115.105:9092,10.85.115.106:9092,10.85.115.107:9092,10.85.115.108:9092,10.85.115.109:9092,10.85.115.110:9092,10.85.115.111:9092,10.85.115.112:9092,10.85.115.113:9092");
//        prop.setProperty("zookeeper.connect", "10.87.52.135:2181,10.87.52.134:2181,10.87.52.158:2181/kafka-0.10.1.1");
        prop.setProperty("group.id", "kafka_system_img_V2_self");
        FlinkKafkaConsumer010<String> kafkaIn010 = new FlinkKafkaConsumer010<String>("system.imgV2", new SimpleStringSchema(), prop);
        kafkaIn010.setStartFromEarliest();//从最早开始消费

        //输出kafka信息
        String producerTopic = "system.imgV2";
        Properties produce_prop = new Properties();
        produce_prop.setProperty("bootstrap.servers", "10.85.115.98:9092,10.85.115.99:9092,10.85.115.100:9092,10.85.115.101:9092,10.85.115.102:9092,10.85.115.103:9092,10.85.115.104:9092,10.85.115.105:9092,10.85.115.106:9092,10.85.115.107:9092,10.85.115.108:9092,10.85.115.109:9092,10.85.115.110:9092,10.85.115.111:9092,10.85.115.112:9092,10.85.115.113:9092");
        FlinkKafkaProducer010<String> kafkaOut010 = new FlinkKafkaProducer010<>(producerTopic, new SimpleStringSchema(), produce_prop, new StringPartitioner<>());
        kafkaOut010.setFlushOnCheckpoint(true);

        int num = Integer.MAX_VALUE;
        img.run(senv,kafkaIn010,kafkaOut010,num);
    }

    public void run(StreamExecutionEnvironment senv, FlinkKafkaConsumer010<String> kafkaIn010, FlinkKafkaProducer010<String> kafkaOut010, int num) throws Exception{
        DataStream<String> img_copy_to_self = senv.addSource(kafkaIn010);
        DataStream<String> stream = img_copy_to_self.map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                for (int j = Integer.MIN_VALUE; j < num; j++){
                    int count = Integer.MIN_VALUE;
                    for (int i = Integer.MIN_VALUE; i < Integer.MAX_VALUE; i++) {
                        count++;
                    }
                }
                for (int j = Integer.MIN_VALUE; j < num; j++){
                    int count = Integer.MIN_VALUE;
                    for (int i = Integer.MIN_VALUE; i < Integer.MAX_VALUE; i++) {
                        count++;
                    }
                }
                return value == null ? "null" : value;
            }
        });
        stream.addSink(kafkaOut010);
//        img_copy_to_self.addSink(new BucketingSink<String>("hdfs://emr-header-1/home/flink/flink_test_zq/img2").setBucketer(new BasePathBucketer<>()));
        senv.execute("kafka_system_img_V2_self");
    }
}
