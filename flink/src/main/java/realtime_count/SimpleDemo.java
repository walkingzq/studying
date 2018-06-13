package realtime_count;

import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.AssignerWithPeriodicWatermarks;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.api.windowing.assigners.GlobalWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.Window;
import org.apache.flink.streaming.connectors.fs.bucketing.BasePathBucketer;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.util.SerializableObject;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import java.io.IOException;
import java.nio.charset.Charset;
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
    private static final Logger LOG = LoggerFactory.getLogger(SimpleDemo.class);//log4j日志对象

    private static final String FWD = "fwd";//转发

    private static final String CMT = "cmt";//评论

    private static final String LK = "lk";//赞

    private static final String OTHER = "other";//其他

    private static final Pattern p_insert_type = Pattern.compile("(insert_type:(\\d*))");//insert_type字段捕获正则表达式

    private static final Pattern p_luicode = Pattern.compile("(luicode:(\\d*))");//luicode字段捕获正则表达式

    private static final Pattern p_uicode = Pattern.compile("(uicode:(\\d*))");//uicode字段捕获正则表达式

    private static final Pattern p_timestamp = Pattern.compile("\\{\"start_time\":\"(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2})");//start_time捕获正则表达式

    //需要设定的参数
    private static long windowSize;//窗口大小，毫秒

    private static int digit;//分组依据：uid倒数第几位

    private static String hdfs_path;//hdfs主路径

    private static String hbase_business;//hbase business

    private static String hbase_keyId;//hbase_keyId


    /**
     * 命令行参数解析
     * @param args
     */
    private static void paramsInit(String[] args){
        if (args.length != 3){
            LOG.error("Invaild params length! Required params: <windowSize> <digit> <hdfs_path>");
            System.exit(1);//退出
        }
        try {
            windowSize = Long.parseLong(args[0]);
        }catch (NumberFormatException exc){
            LOG.error("The input windowSize must be a figure that can be parsed to long type.", exc);
            System.exit(1);//退出
        }
        try {
            digit = Integer.parseInt(args[1]);
        }catch (NumberFormatException exc){
            LOG.error("The input digit must be a figure that can be parsed to int type.", exc);
            System.exit(1);//退出
        }
        hdfs_path = args[2];
        hbase_business = "experimental_results";
        hbase_keyId = "realtime_index_count";
    }


    /**
     * 程序入口
     * @param args （输入一个hdfs路径，作为实时指标统计的备份）
     * @throws Exception
     */
    public static void main(String[] args) throws Exception{
        paramsInit(args);//用户参数解析

        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.enableCheckpointing(5000);//checkpoint间隔:5秒
        senv.setStateBackend(new FsStateBackend("hdfs://emr-cluster/flink/stateBackend"));
        senv.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));//重启策略：如果flink作业失败，flink将自动回退到最近的checkpoint并按照这里设置的重启次数和重启间隔进行作业重启
        senv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);//设定flink内部运算所采用的时间为eventtime

        //输入kafka topic信息（topic名:system.weibo_interact，group.id:business_engine_effect）
        Properties conProp = new Properties();
        conProp.setProperty("bootstrap.servers", "10.77.29.163:9092,10.77.29.164:9092,10.77.31.210:9092,10.77.31.211:9092,10.77.31.212:9092,10.77.29.219:9092,10.77.29.220:9092,10.77.29.221:9092,10.77.29.222:9092,10.77.29.223:9092,10.77.29.224:9092,10.77.29.225:9092");//TODO:kafka broker
        conProp.setProperty("group.id", "business_engine_effect");//TODO:group.id设置
        FlinkKafkaConsumer010<String> kafkaIn010 = new FlinkKafkaConsumer010<String>("system.weibo_interact", new SimpleStringSchema(), conProp);//TODO:消费topic名
        kafkaIn010.setStartFromGroupOffsets();//从group offset处开始消费//TODO:kafka消费起始offset
        kafkaIn010.assignTimestampsAndWatermarks(new AssignerWithPeriodicWatermarks<String>() {//为每条记录设定eventtime和watermark
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


        //开始数据流处理逻辑
        DataStream<String> input = senv.addSource(kafkaIn010);//从kafka接入数据

        //窗口统计--单项指标统计（窗口内转评赞的记录个数）
        DataStream<WindowWordCountEvent> windowWordCount = input.map(new MapFunction<String, WindowWordCountEvent>() {//按照指定规则将每条记录映射为要统计的记录类别（本例中有四类：fwd、cmt、lk和other）
            @Override
            public WindowWordCountEvent map(String value) throws Exception {
                long time = toTimestamp(value.split("\\t")[0]);
                WindowWordCountEvent windowWordCountEvent = new WindowWordCountEvent(time, time, getRecordType(value), 1);
                return windowWordCountEvent;
            }
        }).keyBy("word")//按Word字段进行逻辑分区
                .window(TumblingEventTimeWindows.of(Time.milliseconds(windowSize)))//窗口大小
                .aggregate(new WindowWordCountAggregate(windowSize));//聚合一个窗口内的记录

        //窗口指标聚合--汇总一个窗口内统计的所有指标
        DataStream<RealTimeIndex> indexAggregate = windowWordCount.keyBy("start_time")
                .window(GlobalWindows.create())
                .trigger(new CustomTrigger())
                .aggregate(new RealTimeIndexAggregate());

        //汇总指标输出
        DataStream<String> output = indexAggregate.map(new MapFunction<RealTimeIndex, String>() {
            @Override
            public String map(RealTimeIndex value) throws Exception {
                return value.toJson();
            }
        });

        //单项指标输出
        DataStream<String> output_mul = windowWordCount.map(new MapFunction<WindowWordCountEvent, String>() {
            @Override
            public String map(WindowWordCountEvent value) throws Exception {
                return value.toJson();
            }
        });

        output_mul.addSink(new BucketingSink<String>(hdfs_path + "/mul").setBucketer(new BasePathBucketer<>()));//写入hdfs

        output.addSink(new BucketingSink<String>(hdfs_path + "/total").setBucketer(new BasePathBucketer<>()));//写入hdfs

        output.addSink(new HttpSink<String>(hbase_business, hbase_keyId));//写入hbase
        senv.execute("windowTypeCount");
    }

    /**
     * 自定义http sink
     * @param <IN>
     */
    public static class HttpSink<IN> extends RichSinkFunction<IN>{
        private SerializableObject lock = new SerializableObject();

        private CloseableHttpClient httpClient;

        private HttpPost httpPost;

        private UrlEncodedFormEntity entity;

        private CloseableHttpResponse httpResponse;

        private List<NameValuePair> params;

        private String business;

        private String keyId;

        public HttpSink(){}

        public HttpSink(String business, String keyId){
            this.business = business;
            this.keyId = keyId;
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            try {
                synchronized (lock) {
                    init();
                    createConnection();
                }
            }
            catch (IOException e) {
                throw new IOException("Cannot connect to http server", e);
            }
        }

        /**
         * 初始化参数
         */
        private void init(){
            params = new ArrayList<>();
            params.add(new BasicNameValuePair("business", business));
            params.add(new BasicNameValuePair("keyId", keyId));
        }

        /**
         * 建立http连接
         */
        private void createConnection() throws IOException{
            httpClient = HttpClients.createDefault();
            httpPost = new HttpPost("http://10.77.29.74:8080/waic/hbase/case/insert");
        }

        @Override
        public void invoke(IN value) throws Exception {

        }

        @Override
        public void invoke(IN value, Context context) throws Exception {
            synchronized (lock){
                String str = value.toString();
                long timstamp = System.currentTimeMillis();
                Matcher m = p_timestamp.matcher(str);
                if (m.find()){
                    timstamp = toTimestamp(m.group(1));
                }
                params.add(new BasicNameValuePair("timestamp", String.valueOf(timstamp / 1000)));
                params.add(new BasicNameValuePair("value", str));
                entity = new UrlEncodedFormEntity(params, Charset.forName("utf8"));
                httpPost.setEntity(entity);
                try {
                    httpResponse = httpClient.execute(httpPost);
                    HttpEntity httpEntity = httpResponse.getEntity();
                    System.out.println("insert res:" + EntityUtils.toString(httpEntity));//打印返回结果
                }catch (IOException exc){
                    System.out.println("error in http:" + exc.getMessage());
                }
            }
        }

        @Override
        public void close() throws Exception {
            synchronized (lock){
                try {
                    if (httpClient != null){httpClient.close();}
                    if (httpResponse != null){httpResponse.close();}
                } catch (IOException e) {
                    System.out.println("error in http close:" + e.getMessage());
                }
            }
        }
    }


    /**
     * 自定义Trigger
     */
    public static class CustomTrigger extends Trigger{
        @Override
        public TriggerResult onElement(Object element, long timestamp, Window window, TriggerContext ctx) throws Exception {
            WindowWordCountEvent windowWordCountEvent = (WindowWordCountEvent) element;
            if (windowWordCountEvent.getWord().equals("other")) {
                ctx.registerEventTimeTimer(timestamp + 600 * 1000);//注册一个eventime定时器，当eventtime为timestamp + 600 * 1000时执行onEventTime()方法
            }
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onProcessingTime(long time, Window window, TriggerContext ctx) throws Exception {
            return TriggerResult.CONTINUE;
        }

        @Override
        public TriggerResult onEventTime(long time, Window window, TriggerContext ctx) throws Exception {
            return TriggerResult.FIRE;
        }

        @Override
        public void clear(Window window, TriggerContext ctx) throws Exception {

        }
    }


    /**
     * 将一个指定格式的时间字符串转换成时间戳（ms）
     * @param str （时间格式：yyyy-MM-dd HH:mm:ss）
     * @return 时间戳（ms）
     */
    public static long toTimestamp(String str){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Date date = new Date();
        try {
            date = simpleDateFormat.parse(str);
        }catch (ParseException exc){
            exc.printStackTrace();
        }
        return date.getTime();
    }

    /**
     *将一个timestamp(ms级)转换成固定格式的时间字符串（时间格式：yyyy-MM-dd HH:mm:ss）
     * @param timestamp
     * @return
     */
    public static String toDateStr(long timestamp){
        SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        return simpleDateFormat.format(new Date(timestamp));
    }

    /**
     * 根据规则将一条记录映射成一个类别字符串
     * @param str
     * @return 记录类别（本例中有四类：fwd、cmt、lk和other）
     */
    public static String getRecordType(String str){
        /*
        记录的字段说明：
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
        String uid = strs[2];//uid
        String groupId = "_" + digit + "_null";//groupId
        if (uid.length() > digit){
            groupId = "_" + digit + "_" + uid.charAt(uid.length() - 5);//设定group_id
        }
        String bhv_code = strs[3];//hbv_code
        String mode = strs[5];//mode
        String extend = strs[9];//extend
        Matcher m_insert_type = p_insert_type.matcher(extend);
        String insert_type =  null;//insert_type
        if (m_insert_type.find()){
            insert_type = m_insert_type.group(2);
        }

        Matcher m_luicode = p_luicode.matcher(extend);
        String luicode = null;//luicode
        if (m_luicode.find()){
            luicode = m_luicode.group(2);
        }

        Matcher m_uicode = p_uicode.matcher(extend);
        String uicode = null;//uicode
        if (m_uicode.find()){
            uicode = m_uicode.group(2);
        }

        //判断并标记
        if (insert_type == null || bhv_code == null || !insert_type.equals("5")){return OTHER;}
        if(luicode != null && ( luicode.equals("10000408") || luicode.equals("10000002") ) ){
            if (bhv_code.equals("14000003")){
                return FWD + groupId;//转发
            }else if (bhv_code.equals("14000005")){
                return CMT + groupId;//评论
            }
        }
        if (bhv_code.equals("14000116") && mode != null && mode.equals("1") && uicode != null && (uicode.equals("10000408") || uicode.equals("10000002"))){
            return LK + groupId;//赞
        }
        return OTHER;
    }

    /**
     * 窗口内单项指标统计
     * 统计每个窗口时间内fwd、cmt、lk和other的个数
     */
    public static class WindowWordCountAggregate implements AggregateFunction<WindowWordCountEvent, WindowWordCountEvent, WindowWordCountEvent> {
        private long size;

        public WindowWordCountAggregate(){}

        public WindowWordCountAggregate(long size) {
            this.size = size;
        }

        @Override
        public WindowWordCountEvent createAccumulator() {
            return new WindowWordCountEvent();
        }

        @Override
        public WindowWordCountEvent add(WindowWordCountEvent value, WindowWordCountEvent accumulator) {
            if (accumulator.getStart_time() == 0){return value;}
            WindowWordCountEvent res = new WindowWordCountEvent(Math.min(value.getStart_time(), accumulator.getStart_time()), Math.max(value.getEnd_time(),accumulator.getEnd_time()),value.getWord(), value.getCount() + accumulator.getCount());
            return res;
        }

        @Override
        public WindowWordCountEvent getResult(WindowWordCountEvent accumulator) {
            long timestamp = accumulator.start_time;
            long n = timestamp / size;
            long start_time = size * n;
            WindowWordCountEvent result = new WindowWordCountEvent(start_time, start_time + size - 1, accumulator.getWord(), accumulator.getCount());
            return result;
        }

        @Override
        public WindowWordCountEvent merge(WindowWordCountEvent a, WindowWordCountEvent b) {
            WindowWordCountEvent res = new WindowWordCountEvent(Math.min(a.getStart_time(), b.getStart_time()), Math.max(a.getEnd_time(),b.getEnd_time()),a.getWord(),a.getCount() + b.getCount());
            return res;
        }
    }


    /**
     * 窗口内多个指标聚合成一个
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

    /**
     * 单项指标统计的event
     */
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
            return start_time + "," +
                    end_time + "," +
                    word + "," +
                    count;
        }
    }


    /**
     * 汇总指标统计的event
     */
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

        /**
         * 将传入的键值put到实例对象的指标map中
         * @param index
         * @param count
         */
        public void putIndex(String index, Long count){
            this.indexs.put(index, count);
        }

        /**
         * 将传入的指标map加入实例对象的指标map中
         * @param indexsToAdd
         */
        public void addIndex(Map<String, Long> indexsToAdd){
            for (Map.Entry<String, Long> entry:indexsToAdd.entrySet()
                 ) {
                this.indexs.put(entry.getKey(), entry.getValue());
            }
        }

        /**
         * 将实例对象转换成一个json字符串
         * @return json字符串
         */
        public String toJson(){
            StringBuilder sb = new StringBuilder();
            sb.append("{\"start_time\":\"" + toDateStr(this.start_time)).append("\",")
                    .append("\"end_time\":\"" + toDateStr(this.end_time)).append("\",");
            Set<Map.Entry<String, Long>> entrySet = this.indexs.entrySet();
            sb.append("\"indexs\":{");
            int i = 0 , len = entrySet.size();
            for (Map.Entry<String, Long> entry : entrySet){
                sb.append("\"" + entry.getKey() + "\":\"" + entry.getValue());
                if (i < len - 1){sb.append("\",");}
                else {sb.append("\"}");}
                i++;
            }
            sb.append("}");
            return sb.toString();
        }
    }
}
