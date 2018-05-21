package checkingout;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.util.Collector;
import partitioner.SimplePartitioner;

import java.util.Properties;

/**
 * Created by Zhao Qing on 2018/5/21.
 * checkingout.DelayTesting
 */
public class DelayTesting {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();

        //输入kafka信息（kafka010版本）
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "10.87.52.135:9092,10.87.52.134:9092,10.87.52.158:9092");
        prop.setProperty("zookeeper.connect", "10.87.52.135:2181,10.87.52.134:2181,10.87.52.158:2181/kafka-0.10.1.1");
        prop.setProperty("group.id", "simpleDemo");
        FlinkKafkaConsumer010<String> kafkaIn010 = new FlinkKafkaConsumer010<String>("system.pic_todownload_ali_01", new SimpleStringSchema(), prop);

        //输出kafka信息
        String producerTopic = "basetest";
        Properties produce_prop = new Properties();
        produce_prop.setProperty("bootstrap.servers", "10.87.52.135:9092,10.87.52.134:9092,10.87.52.158:9092");
        FlinkKafkaProducer010<String> kafkaOut010 = new FlinkKafkaProducer010<String>( producerTopic, new SimpleStringSchema(), produce_prop, new SimplePartitioner<>());

        DataStream<String> source = senv.addSource(kafkaIn010).map(new MapFunction<String, String>() {
            @Override
            public String map(String value) throws Exception {
                return System.currentTimeMillis() + "," + value;
            }
        });//添加in_time,数据格式:in_time value

        DataStream<WordWithCount> wordcount = source.flatMap(new FlatMapFunction<String, WordWithCount>() {
            @Override
            public void flatMap(String value, Collector<WordWithCount> out) throws Exception {
                String in_time = value.substring(0,13);
                String[] strs = value.substring(14).split(",");
                for (String str:strs
                        ) {
                    out.collect(new WordWithCount(Long.parseLong(in_time), str, 1 ));
                }
            }
        })//分词，数据格式：in_time word 1
                .keyBy("word")
                .timeWindow(Time.seconds(5), Time.seconds(1))
                .reduce(new ReduceFunction<WordWithCount>() {
                    @Override
                    public WordWithCount reduce(WordWithCount a, WordWithCount b) {
                        return new WordWithCount(a.in_time, a.word, a.count + b.count);
                    }
                });
        DataStream<String> output = wordcount.map(new MapFunction<WordWithCount, String>() {
            @Override
            public String map(WordWithCount value) throws Exception {
                return value.toString() + "," + System.currentTimeMillis();
            }
        });

        //添加out_time，数据格式：in_time out_time word 1
        //时延:out_time-in_time,kafka的timestamp-in_time
        output.addSink(kafkaOut010);
//        img_copy_to_self.addSink(new BucketingSink<String>("hdfs://emr-header-1/home/flink/flink_test_zq/img2").setBucketer(new BasePathBucketer<>()));
        senv.execute("flink_simple_demo");
    }

    public static class WordWithCount {
        public long in_time;
        public String word;
        public long count;

        public WordWithCount() {}

        public WordWithCount(long in_time, String word, long count) {
            this.in_time = in_time;
            this.word = word;
            this.count = count;
        }


        @Override
        public String toString() {
            return word + " : " + count;
        }
    }
}
