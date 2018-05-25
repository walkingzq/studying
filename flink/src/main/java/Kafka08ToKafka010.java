import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.bucketing.BasePathBucketer;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer08;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.log4j.Logger;
import partitioner.StringPartitioner;

import java.util.Properties;

/**
 * Created by Zhao Qing on 2018/5/15.
 */
public class Kafka08ToKafka010 {

    private static Logger LOGGER = Logger.getLogger(Kafka08ToKafka010.class);

    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.enableCheckpointing(5000);
        senv.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));

        //接入kafka信息
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty("bootstrap.servers", "10.85.12.218:9092,10.85.12.219:9092,10.85.12.220:9092,10.85.12.221:9092,10.85.12.227:9092");
        consumerProperties.setProperty("zookeeper.connect", "10.85.12.218:2181,10.85.12.219:2181,10.85.12.220:2181,10.85.12.221:2181,10.85.12.227:2181");
        consumerProperties.setProperty("group.id", "img_in");
        FlinkKafkaConsumer08<String> kafkaIn08 = new FlinkKafkaConsumer08<String>("system.pic_todownload_ali", new SimpleStringSchema(), consumerProperties);

        //输出kafka信息
        Properties producerProp = new Properties();
        producerProp.setProperty("bootstrap.servers", "10.87.52.135:9092,10.87.52.134:9092,10.87.52.158:9092");
        String producerTopic = "system.pic_todownload_ali_01";
        FlinkKafkaProducer010<String> kafkaOut010 = new FlinkKafkaProducer010<>(producerTopic, new SimpleStringSchema(), producerProp, new StringPartitioner<>());
        kafkaOut010.setFlushOnCheckpoint(true);

        DataStream<String> img = senv.addSource(kafkaIn08);
        img.addSink(kafkaOut010);
        senv.execute("img_in");
    }
}
