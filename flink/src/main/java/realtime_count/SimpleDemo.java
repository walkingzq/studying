package realtime_count;

import org.apache.flink.api.common.functions.AggregateFunction;
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

import javax.annotation.Nullable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Create by Zhao Qing on 2018/6/4
 * realtime_count.SimpleDemo
 */
public class SimpleDemo {
    private static final String FWD = "fwd";//转发

    private static final String CMT = "cmt";//评论

    private static final String LK = "lk";//赞

    private static final String OTHER = "other";//其他

    private static final Pattern p_insert_type = Pattern.compile("(insert_type:(\\d*))");

    private static final Pattern p_luicode = Pattern.compile("(luicode:(\\d*))");

    private static final Pattern p_uicode = Pattern.compile("(uicode:(\\d*))");

    public static void main(String[] args) throws Exception{
        String hdfs_path = args[0];

        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.enableCheckpointing(5000);
        senv.setStateBackend(new FsStateBackend("hdfs://emr-cluster/flink/stateBackend"));
        senv.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        Properties conProp = new Properties();
        conProp.setProperty("bootstrap.servers", "10.77.29.163:9092,10.77.29.164:9092,10.77.31.210:9092,10.77.31.211:9092,10.77.31.212:9092,10.77.29.219:9092,10.77.29.220:9092,10.77.29.221:9092,10.77.29.222:9092,10.77.29.223:9092,10.77.29.224:9092,10.77.29.225:9092");
        conProp.setProperty("group.id", "windowWordCount-ver4");//TODO:group_id 待确认 @陈超
        FlinkKafkaConsumer010<String> kafkaIn010 = new FlinkKafkaConsumer010<String>("system.weibo_interact", new SimpleStringSchema(), conProp);
        kafkaIn010.setStartFromGroupOffsets();
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
                long timestamp = toTimestamp(element.split("\\t")[0]);
                currentMaxTimestamp = Math.max(timestamp, currentMaxTimestamp);
                return timestamp;
            }
        });


        DataStream<String> input = senv.addSource(kafkaIn010);

        DataStream<WindowWordCountEvent> windowWordCount = input.map(new MapFunction<String, WindowWordCountEvent>() {
            @Override
            public WindowWordCountEvent map(String value) throws Exception {
                long time = toTimestamp(value);
                return new WindowWordCountEvent(time, time, getRecordType(value), 1);
            }
        }).keyBy("word")
                .window(TumblingEventTimeWindows.of(Time.seconds(10 * 60)))
                .aggregate(new WindowWordCountAggregate());

        DataStream<String> output = windowWordCount.map(new MapFunction<WindowWordCountEvent, String>() {
            @Override
            public String map(WindowWordCountEvent value) throws Exception {
                return value.toJson();
            }
        });

        output.addSink(new BucketingSink<String>(hdfs_path).setBucketer(new BasePathBucketer<>()));

        senv.execute("windowTypeCount");

    }

    public static long toTimestamp(String str){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date();
        try {
            date = simpleDateFormat.parse(str);
        }catch (ParseException exc){
            exc.printStackTrace();
        }
        return date.getTime() / 1000;
    }

    public static String toDateStr(long timestamp){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return simpleDateFormat.format(new Date(timestamp * 1000));
    }

    public static String getRecordType(String str){
        /*
        0 time
        1 ip
        2 uid
        3 bhv_code
        4 appid
        5 mode
        6 status
        7 object
        8 opp_uid
        9 extend
         */
        String[] strs = str.split("\\t");
        String bhv_code = strs[3];
        String mode = strs[5];
        String extend = strs[9];
        Matcher m_insert_type = p_insert_type.matcher(extend);
        String insert_type =  null;
        System.out.println("bhv_code:" + bhv_code + ",mode:" + mode +",extend:");
        if (m_insert_type.find()){
            insert_type = m_insert_type.group(2);
        }

        Matcher m_luicode = p_luicode.matcher(extend);
        String luicode = null;
        if (m_luicode.find()){
            luicode = m_luicode.group(2);
        }

        Matcher m_uicode = p_uicode.matcher(extend);
        String uicode = null;
        if (m_uicode.find()){
            uicode = m_uicode.group(2);
        }

        if (insert_type == null || bhv_code == null || !insert_type.equals("5")){return OTHER;}
        if(luicode != null && ( luicode.equals("10000408") || luicode.equals("10000002") ) ){
            if (bhv_code.equals("14000003")){
                return FWD;//转发
            }else if (bhv_code.equals("14000005")){
                return CMT;//评论
            }
        }
        if (bhv_code.equals("14000116") && mode != null && mode.equals("1") && uicode != null && (uicode.equals("10000408") || uicode.equals("10000002"))){
            return LK;//赞
        }
        return OTHER;
    }

    public static class WindowWordCountAggregate implements AggregateFunction<WindowWordCountEvent, WindowWordCountEvent, WindowWordCountEvent> {
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

        public String toJson(){
            return "{\"start_time\":\"" + toDateStr(start_time) + "\"" + "," +
                    "\"end_time\":\"" + toDateStr(end_time) + "\"" + "," +
                    "\"type\":\"" + word + "\"" + "," +
                    "\"count\":\"" + count + "\"" +
                    "}";
        }

        @Override
        public String toString(){
            return toDateStr(start_time) + "," +
                    toDateStr(end_time) + "," +
                    word + "," +
                    count;
        }
    }
}
