package checkingout.exactlyOne;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.fs.bucketing.BasePathBucketer;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.Collector;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * Created by Zhao Qing on 2018/5/25.
 * checkingout.exactlyOne.WindowWordCount
 */
public class WindowWordCount {
    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.enableCheckpointing(5000);
        senv.setStateBackend(new FsStateBackend("hdfs://emr-header-1/flink/stateBackend"));
        senv.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties conProp = new Properties();
        conProp.setProperty("bootstrap.servers", "10.87.52.135:9092,10.87.52.134:9092,10.87.52.158:9092");
        conProp.setProperty("group.id", "exactlyOneTesting");
        FlinkKafkaConsumer010<String> kafkaIn010 = new FlinkKafkaConsumer010<String>("exaclyOne", new SimpleStringSchema(), conProp);
        kafkaIn010.setStartFromEarliest();
        kafkaIn010.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {
            private final long maxOutOdOrderTime = 3500;

            private long currentMaxTimestamp = Long.MAX_VALUE;

            @Nullable
            @Override
            public Watermark getCurrentWatermark() {
                return new Watermark(currentMaxTimestamp - maxOutOdOrderTime);
            }

            @Override
            public long extractTimestamp(String element, long previousElementTimestamp) {
                long timestamp = Long.parseLong(element.split(",")[0]);
                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                return timestamp;
            }
        });

        DataStream<String> input = senv.addSource(kafkaIn010);

        DataStream<WindowWordCountEvent> wordcount = input.flatMap(new FlatMapFunction<String, WindowWordCountEvent>() {
            @Override
            public void flatMap(String value, Collector<WindowWordCountEvent> out) throws Exception {
                String[] words = value.split(",");
                for (int i = 0; i < words.length; i++){
                    out.collect(new WindowWordCountEvent(words[i], 1));
                }
            }
        }).keyBy("word")
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .sum("count");

        DataStream<String> output = wordcount.map(new MapFunction<WindowWordCountEvent, String>() {
            @Override
            public String map(WindowWordCountEvent value) throws Exception {
                return value.toString();
            }
        });

        output.addSink(new BucketingSink<String>("hdfs://emr-header-1/home/flink/testing/exaclyone/ver1/data").setBucketer(new BasePathBucketer<>()));
        senv.execute("exactlyOneTesting");


    }

    public static class WindowWordCountEvent{
        private String word;

        private long count;

        public WindowWordCountEvent(){}

        public WindowWordCountEvent(String word, long count) {
            this.word = word;
            this.count = count;
        }

        public String getWord() {
            return word;
        }

        public void setWord(String word) {
            this.word = word;
        }

        public long getCount() {
            return count;
        }

        public void setCount(long count) {
            this.count = count;
        }

        @Override
        public String toString(){
            return word + "," +
                    count;
        }
    }
}
