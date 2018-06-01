package checkingout.exactlyOne;

import org.apache.flink.api.common.functions.AggregateFunction;
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
import org.apache.log4j.Logger;

import javax.annotation.Nullable;
import java.util.Properties;

/**
 * Created by Zhao Qing on 2018/5/25.
 * checkingout.exactlyOne.WindowWordCount
 */
public class WindowWordCount {
    private static Logger LOGGER = Logger.getLogger(WindowWordCount.class);

    public static void main(String[] args) throws Exception{
        if (args.length != 1){LOGGER.info("usage: <jar_name> <hdfs_relative_path>");}
        String hdfs_path = "hdfs://emr-cluster" + args[0];
        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.enableCheckpointing(5000);
        senv.setStateBackend(new FsStateBackend("hdfs://emr-cluster/flink/stateBackend"));
        senv.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties conProp = new Properties();
        conProp.setProperty("bootstrap.servers", "10.87.52.135:9092,10.87.52.134:9092,10.87.52.158:9092");
        conProp.setProperty("group.id", "windowWordCount-ver4");
        FlinkKafkaConsumer010<String> kafkaIn010 = new FlinkKafkaConsumer010<String>("exaclyOne", new SimpleStringSchema(), conProp);
        kafkaIn010.setStartFromEarliest();
        kafkaIn010.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {
            private final long maxOutOdOrderTime = 3500;

            private long currentMaxTimestamp;

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
                long word_time = Long.parseLong(words[0]);//时间
                for (int i = 1; i < words.length; i++){
                    out.collect(new WindowWordCountEvent(word_time, word_time,words[i], 1));
                }
            }
        }).keyBy("word")
                .window(TumblingEventTimeWindows.of(Time.seconds(10)))
                .aggregate(new WindowWordCountAggregate());

        DataStream<String> output = wordcount.map(new MapFunction<WindowWordCountEvent, String>() {
            @Override
            public String map(WindowWordCountEvent value) throws Exception {
                return value.toString();
            }
        });

        output.addSink(new BucketingSink<String>(hdfs_path).setBucketer(new BasePathBucketer<>()));

        senv.execute("windowWordCount");


    }

    public static class WindowWordCountAggregate implements AggregateFunction<WindowWordCountEvent, WindowWordCountEvent, WindowWordCountEvent>{
        @Override
        public WindowWordCountEvent createAccumulator() {
            return new WindowWordCountEvent();
        }

        @Override
        public WindowWordCountEvent add(WindowWordCountEvent value, WindowWordCountEvent accumulator) {
            if (accumulator.getStart_time() == 0){return value;}
            return new WindowWordCountEvent(Math.min(value.getStart_time(), accumulator.getStart_time()), Math.max(value.getEnd_time(),accumulator.getEnd_time()),value.getWord(), value.getCount() + accumulator.getCount());
        }

        @Override
        public WindowWordCountEvent getResult(WindowWordCountEvent accumulator) {
            return accumulator;
        }

        @Override
        public WindowWordCountEvent merge(WindowWordCountEvent a, WindowWordCountEvent b) {
            return new WindowWordCountEvent(Math.min(a.getStart_time(), b.getStart_time()), Math.max(a.getEnd_time(),b.getEnd_time()),a.getWord(),a.getCount() + b.getCount());
        }
    }


    public static class WindowWordCountEvent{
        private long start_time;

        private long end_time;

        private String word;

        private long count;

        public WindowWordCountEvent(){}

        public WindowWordCountEvent(long start_time, long end_time, String word, long count) {
            this.start_time = start_time;
            this.end_time = end_time;
            this.word = word;
            this.count = count;
        }

        public long getStart_time() {
            return start_time;
        }

        public void setStart_time(long start_time) {
            this.start_time = start_time;
        }

        public long getEnd_time() {
            return end_time;
        }

        public void setEnd_time(long end_time) {
            this.end_time = end_time;
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
            return start_time + "," +
                    end_time + "," +
                    word + "," +
                    count;
        }
    }
}
