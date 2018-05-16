package checkingout;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.bucketing.BasePathBucketer;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * Created by Zhao Qing on 2018/5/16.
 * 内容：吞吐量测试
 * 思路：接入一个无限量数据（相对而言）的kafka topic作为flink输入，flink在收到数据后添加一个时间戳就输出至另一个kafka topic和hdfs
 */
public class ThroughPutTesting {
    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        //输入kafka信息（kafka010版本）
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "10.87.52.135:9092,10.87.52.134:9092,10.87.52.158:9092");
        prop.setProperty("zookeeper.connect", "10.87.52.135:2181,10.87.52.134:2181,10.87.52.158:2181/kafka-0.10.1.1");
        prop.setProperty("group.id", "throughput_testing_01");
        FlinkKafkaConsumer010<String> kafkaIn010 = new FlinkKafkaConsumer010<String>("system.pic_todownload_ali_01", new SimpleStringSchema(), prop);
        kafkaIn010.setStartFromEarliest();//从最早开始读取

        //输出kafka信息（kafka010版本）
        String broker = "10.87.52.135:9092,10.87.52.134:9092,10.87.52.158:9092";
        String producerTopic = "throughput_testing_01";
        FlinkKafkaProducer010<String> kafkaOut010 = new FlinkKafkaProducer010<>(broker, producerTopic, new SimpleStringSchema());

        run(senv,kafkaIn010,kafkaOut010);

    }

    private static void run(StreamExecutionEnvironment senv, FlinkKafkaConsumer010 kafkaIn010, FlinkKafkaProducer010 kafkaOut010) throws Exception{
        senv.enableCheckpointing(1000);//Exactly one,1000ms设置一个checkpoint
        DataStream<String> stream = senv.addSource(kafkaIn010);
        DataStream<String> streamWithTime = stream.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                out.collect(System.currentTimeMillis() + " " + "throughput_testing_01" + " " + value);
            }
        });
        streamWithTime.addSink(new BucketingSink<String>("hdfs://emr-header-1/home/flink/flink_test_zq/throughput_testing_01").setBucketer(new BasePathBucketer<>()));
        streamWithTime.addSink(kafkaOut010);
        senv.execute("ThroughPutTesting1");
    }
}
