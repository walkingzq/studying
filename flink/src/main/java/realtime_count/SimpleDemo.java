package realtime_count;

import com.sun.javafx.collections.MappingChange;
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
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.GlobalWindow;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.fs.bucketing.BasePathBucketer;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import javax.annotation.Nullable;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
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
        conProp.setProperty("group.id", "business_engine_effect");
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
                .window(TumblingEventTimeWindows.of(Time.seconds(10 * 60)))//窗口大小:10min
                .aggregate(new WindowWordCountAggregate());

//        DataStream<RealTimeIndex> indexAggregate = windowWordCount.keyBy("start_time")
//                .window(GlobalWindows.create())
//                .trigger(new CustomTrigger())
//                .aggregate(new RealTimeIndexAggregate());//TODO:Trigger定义
//
//        DataStream<String> output = indexAggregate.map(new MapFunction<RealTimeIndex, String>() {
//            @Override
//            public String map(RealTimeIndex value) throws Exception {
//                return value.toJson();
//            }
//        });

        DataStream<String> output_mul = windowWordCount.map(new MapFunction<WindowWordCountEvent, String>() {
            @Override
            public String map(WindowWordCountEvent value) throws Exception {
                return value.toJson();
            }
        });

        output_mul.addSink(new BucketingSink<String>(hdfs_path).setBucketer(new BasePathBucketer<>()));

//        output.addSink(new BucketingSink<String>(hdfs_path).setBucketer(new BasePathBucketer<>()));

        senv.execute("windowTypeCount");

    }


    /**
     * 自定义Trigger
     */
    public static class CustomTrigger extends Trigger{
        @Override
        public TriggerResult onElement(Object element, long timestamp, Window window, TriggerContext ctx) throws Exception {
            return TriggerResult.FIRE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, Window window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, Window window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public void clear(Window window, TriggerContext ctx) throws Exception {

        }
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

    /**
     * 窗口词频统计
     */
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


    /**
     * 时间窗口合并
     */
    public static class RealTimeIndexAggregate implements AggregateFunction<WindowWordCountEvent, RealTimeIndex, RealTimeIndex>{
        @Override
        public RealTimeIndex createAccumulator() {
            return new RealTimeIndex();
        }

        @Override
        public RealTimeIndex add(WindowWordCountEvent value, RealTimeIndex accumulator) {
            accumulator.setStart_time(value.getStart_time());
            accumulator.setEnd_time(value.getEnd_time());
            accumulator.putIndex(value.getWord(), value.getCount());
            return accumulator;
        }

        @Override
        public RealTimeIndex getResult(RealTimeIndex accumulator) {
            return accumulator;
        }

        @Override
        public RealTimeIndex merge(RealTimeIndex a, RealTimeIndex b) {
            a.addIndex(b.getIndexs());
            return a;
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

    public static class RealTimeIndex{
        private long start_time;

        private long end_time;

        private Map<String, Long> indexs;

        public RealTimeIndex(){
            this.indexs = new HashMap<>();
        }

        public RealTimeIndex(long start_time, long end_time, Map<String, Long> indexs) {
            this.start_time = start_time;
            this.end_time = end_time;
            this.indexs = indexs;
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

        public Map<String, Long> getIndexs() {
            return indexs;
        }

        public void setIndexs(Map<String, Long> indexs) {
            this.indexs = indexs;
        }

        public void putIndex(String index, Long count){
            this.indexs.put(index, count);
        }

        public void addIndex(Map<String, Long> indexsToAdd){
            for (Map.Entry<String, Long> entry:indexsToAdd.entrySet()
                 ) {
                this.indexs.put(entry.getKey(), entry.getValue());
            }
        }

        public String toJson(){
            StringBuilder sb = new StringBuilder();
            sb.append("{\"start_time\":\"" + toDateStr(this.start_time)).append("\",")
                    .append("\"end_time\":\"" + toDateStr(this.end_time)).append("\",");
            Set<Map.Entry<String, Long>> entrySet = this.indexs.entrySet();
            int i = 0 , len = entrySet.size();
            for (Map.Entry<String, Long> entry : entrySet){
                sb.append("\"" + entry.getKey() + "\":\"" + entry.getValue());
                if (i < len - 1){sb.append("\",");}
                else {sb.append("\"}");}
                i++;
            }
            return sb.toString();
        }
    }
}
