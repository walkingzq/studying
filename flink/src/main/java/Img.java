import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * Created by Zhao Qing on 2018/5/15.
 */
public class Img {
    public static void main(String[] args) throws Exception{

        Img img = new Img();

        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        //输入kafka信息（kafka010版本）
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "10.87.52.135:9092,10.87.52.134:9092,10.87.52.158:9092");
        prop.setProperty("zookeeper.connect", "10.87.52.135:2181,10.87.52.134:2181,10.87.52.158:2181/kafka-0.10.1.1");
        prop.setProperty("group.id", "img_copy_to_self");
        FlinkKafkaConsumer010<String> kafkaIn010 = new FlinkKafkaConsumer010<String>("system.pic_todownload_ali_01", new SimpleStringSchema(), prop);

        //输出kafka信息
        String broker = "10.87.52.135:9092,10.87.52.134:9092,10.87.52.158:9092";
        String producerTopic = "system.pic_todownload_ali_01";
        FlinkKafkaProducer010<String> kafkaOut010 = new FlinkKafkaProducer010<>(broker, producerTopic, new SimpleStringSchema());

        int num = args[0] == null ? 5 : Integer.parseInt(args[0]);
        img.run(senv,kafkaIn010,kafkaOut010,num);
    }

    public void run(StreamExecutionEnvironment senv, FlinkKafkaConsumer010<String> kafkaIn010, FlinkKafkaProducer010<String> kafkaOut010, int num) throws Exception{
        DataStream<String> img_copy_to_self = senv.addSource(kafkaIn010);
        DataStream<String> stream = img_copy_to_self.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                for (int j = 0; j < num; j++){
                    int count = Integer.MIN_VALUE;
                    for (int i = Integer.MIN_VALUE; i < Integer.MAX_VALUE; i++) {
                        count++;
                    }
                }
                out.collect(value);
            }
        });
        stream.addSink(kafkaOut010);
//        img_copy_to_self.addSink(new BucketingSink<String>("hdfs://emr-header-1/home/flink/flink_test_zq/img2").setBucketer(new BasePathBucketer<>()));
        senv.execute("img");
    }
}
