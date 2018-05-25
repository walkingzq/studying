import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.serialization.TypeInformationSerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.util.Collector;
import partitioner.SimplePartitioner;
import partitioner.StringPartitioner;

import java.util.Properties;

/**
 * Create by Zhao Qing on 2018/5/16
 */
public class FlinkSimpleDemo {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.enableCheckpointing(1000);
        senv.setRestartStrategy(new RestartStrategies.NoRestartStrategyConfiguration());

        //输入kafka信息（kafka010版本）
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "10.87.52.135:9092,10.87.52.134:9092,10.87.52.158:9092");
        prop.setProperty("group.id", "simpleDemo");
        FlinkKafkaConsumer010<String> kafkaIn010 = new FlinkKafkaConsumer010<String>("topic2", new SimpleStringSchema(), prop);
        kafkaIn010.setStartFromLatest();

        //输出kafka信息
        String producerTopic = "basetest";
        Properties produce_prop = new Properties();
        produce_prop.setProperty("bootstrap.servers", "10.87.52.135:9092,10.87.52.134:9092,10.87.52.158:9092");
        TypeInformation<Tuple4<Long, Long, String, Integer>> typeInformation = TypeInformation.of(new TypeHint<Tuple4<Long, Long, String, Integer>>(){});
        ExecutionConfig ec = senv.getConfig();
        TypeInformationSerializationSchema<Tuple4<Long, Long, String, Integer>> serializationSchema = new TypeInformationSerializationSchema<>(typeInformation,ec);
        FlinkKafkaProducer010<Tuple4<Long, Long, String, Integer>> kafkaOut010 = new FlinkKafkaProducer010<>( producerTopic, serializationSchema, produce_prop, new StringPartitioner<>());
        kafkaOut010.setFlushOnCheckpoint(true);

//        DataStream<Tuple2<Long,String>> source = senv.addSource(kafkaIn010).map(new MapFunction<String, Tuple2<Long, String>>() {
//            @Override
//            public Tuple2<Long, String> map(String value) throws Exception {
//                return new Tuple2<Long,String>(System.currentTimeMillis(),value);
//            }
//        }).returns(new TypeHint<Tuple2<Long, String>>(){});//添加in_time,数据格式:in_time value

//        DataStream<Tuple3<Long,String,Integer>> wordcount = source.flatMap(new FlatMapFunction<Tuple2<Long, String>, Tuple3<Long, String, Integer>>() {
//            @Override
//            public void flatMap(Tuple2<Long, String> value, Collector<Tuple3<Long, String, Integer>> out) throws Exception {
//                Long in_time = value.f0;
//                String[] strs = value.f1.split(",");
//                for (String str:strs
//                     ) {
//                    out.collect(new Tuple3<Long,String,Integer>(in_time, str, 1));
//                }
//            }
//        }).returns(new TypeHint<Tuple3<Long, String, Integer>>(){});//分词，数据格式：in_time word 1

        DataStream<Tuple4<Long,Long,String,Integer>> output = senv.addSource(kafkaIn010).map(new MapFunction<String, Tuple4<Long,Long,String,Integer>>() {
            @Override
            public Tuple4 map(String value) throws Exception {
                return new Tuple4<Long,Long,String,Integer>(System.currentTimeMillis(),System.currentTimeMillis(),value,1);
            }
        }).returns(new TypeHint<Tuple4<Long, Long, String, Integer>>(){});

//        DataStream<Tuple4<Long,Long,String,Integer>> output = wordcount.map(new MapFunction<Tuple3<Long, String, Integer>, Tuple4<Long, Long, String, Integer>>() {
//            @Override
//            public Tuple4<Long, Long, String, Integer> map(Tuple3<Long, String, Integer> value) throws Exception {
//                return new Tuple4<Long, Long, String, Integer>(value.f0, System.currentTimeMillis(), value.f1, value.f2);
//            }
//        }).returns(new TypeHint<Tuple4<Long, Long, String, Integer>>(){});//添加out_time，数据格式：in_time out_time word 1
        //时延:out_time-in_time,kafka的timestamp-in_time
        output.addSink(kafkaOut010);
//        img_copy_to_self.addSink(new BucketingSink<String>("hdfs://emr-header-1/home/flink/flink_test_zq/img2").setBucketer(new BasePathBucketer<>()));
        senv.execute("flink_simple_demo");
    }
}
