import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.util.Collector;

import java.util.Properties;

/**
 * Create by Zhao Qing on 2018/5/16
 */
public class FlinkSimpleDemo {
    public static void main(String[] args) throws Exception {
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

        TypeInformation<Tuple2<String, Integer>> typeInformation = TypeInformation.of(new TypeHint<Tuple2<String,Integer>>(){});
        ExecutionConfig ec = new ExecutionConfig();
        TypeInformationSerializationSchema<Tuple2<String, Integer>> serializationSchema = new TypeInformationSerializationSchema<>(typeInformation,ec);
        FlinkKafkaProducer010<Tuple2<String, Integer>> kafkaOut010 = new FlinkKafkaProducer010<Tuple2<String, Integer>>(broker, producerTopic, serializationSchema);
    }

    public void run(StreamExecutionEnvironment senv, FlinkKafkaConsumer010<String> kafkaIn010, FlinkKafkaProducer010<Tuple2<String, Integer>> kafkaOut010) throws Exception{
        DataStream<String> source = senv.addSource(kafkaIn010);
        DataStream<Tuple2<String, Integer>> stream = source.flatMap(new FlatMapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public void flatMap(String value, Collector<Tuple2<String, Integer>> out) throws Exception {
                out.collect(new Tuple2<>(value,1));
            }
        });
        stream.addSink(kafkaOut010);
//        img_copy_to_self.addSink(new BucketingSink<String>("hdfs://emr-header-1/home/flink/flink_test_zq/img2").setBucketer(new BasePathBucketer<>()));
        senv.execute("flink_simple_demo");
    }
}
