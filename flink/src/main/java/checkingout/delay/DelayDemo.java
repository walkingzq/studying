package checkingout.delay;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.bucketing.BasePathBucketer;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.util.Collector;
import partitioner.StringPartitioner;

import java.util.Properties;

/**
 * Created by Zhao Qing on 2018/5/24.
 * checkingout.delay.DelayDemo
 */
public class DelayDemo {
    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.enableCheckpointing(5000);
        senv.setStateBackend(new FsStateBackend("hdfs://emr-header-1/flink/checkpoints_zq"));
        senv.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
//        senv.getConfig().disableSysoutLogging();
        senv.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        Properties conProp = new Properties();
        conProp.setProperty("bootstrap.servers", "10.87.52.135:9092,10.87.52.134:9092,10.87.52.158:9092");
        conProp.setProperty("group.id", "delayTesting");
        FlinkKafkaConsumer010<String> kafkaIn010 = new FlinkKafkaConsumer010<String>("system.pic_todownload_ali_01", new SimpleStringSchema(), conProp);
        kafkaIn010.setStartFromEarliest();

        Properties produProp = new Properties();
        produProp.setProperty("bootstrap.servers", "10.87.52.135:9092,10.87.52.134:9092,10.87.52.158:9092");
        FlinkKafkaProducer010<DelayEvent> kafkaOut010 = new FlinkKafkaProducer010<DelayEvent>("delayTesting01", new DelayEventSchema(), produProp, new StringPartitioner<>());
        kafkaOut010.setFlushOnCheckpoint(true);

        DataStream<DelayEventWithInTime> input = senv.addSource(kafkaIn010)
                .flatMap(new FlatMapFunction<String, DelayEventWithInTime>() {
                    @Override
                    public void flatMap(String value, Collector<DelayEventWithInTime> out) throws Exception {
                        long in_time = System.currentTimeMillis();
                        for (String word : value.split(",")){
                            out.collect(new DelayEventWithInTime(in_time, word, 1));
                        }
                    }
                }).keyBy("value")
//                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<DelayEventWithInTime>() {
                    @Override
                    public DelayEventWithInTime reduce(DelayEventWithInTime value1, DelayEventWithInTime value2) throws Exception {
                        long in_time = value1.getIn_time() >= value2.getIn_time() ? value1.getIn_time() : value2.getIn_time();
                        return new DelayEventWithInTime(in_time, value1.getValue(), value1.getCount() + value2.getCount());
                    }
                });

        DataStream<DelayEvent> output = input.map(new MapFunction<DelayEventWithInTime, DelayEvent>() {
            @Override
            public DelayEvent map(DelayEventWithInTime value) throws Exception {
                return new DelayEvent(value.getValue(), value.getIn_time(), System.currentTimeMillis(), value.getCount());
            }
        });

//        output.addSink(kafkaOut010);
        output.addSink(new BucketingSink<DelayEvent>("hdfs://emr-header-1/home/flink/testing/delay/ver2/data").setBucketer(new BasePathBucketer<>()));
        try {
            senv.execute("delaytesting");
        }catch (Exception exc){
            throw exc;
        }
    }
}
