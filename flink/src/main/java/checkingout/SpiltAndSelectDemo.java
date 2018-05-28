package checkingout;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.collector.selector.OutputSelector;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SplitStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.bucketing.BasePathBucketer;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.log4j.Logger;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * Created by Zhao Qing on 2018/5/28.
 * checkingout.SpiltAndSelectDemo
 */
public class SpiltAndSelectDemo {
    private static final Logger LOGGER = Logger.getLogger(SpiltAndSelectDemo.class);

    public static void main(String[] args) throws Exception{
        if (args.length != 2){LOGGER.info("usage:<jar_name> <hdfs_dir> <versionId>");}
        String versionId = args[1];
        String hdfsDir = args[0] + "/ver" + versionId + "/data";
        LOGGER.info("hdfs主目录:" + hdfsDir);
        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.enableCheckpointing(5000);
        try {
            senv.setStateBackend(new FsStateBackend("hdfs://emr-header-1/flink/checkpoints_zq"));
        }catch (IOException exc){
            throw exc;
        }
        senv.setRestartStrategy(RestartStrategies.fixedDelayRestart(4, 10000));
        senv.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);

        Properties conProp = new Properties();
        conProp.setProperty("bootstrap.servers", "10.87.52.135:9092,10.87.52.134:9092,10.87.52.158:9092");
        conProp.setProperty("group.id", "delayTesting");
        FlinkKafkaConsumer010<String> kafkaIn010 = new FlinkKafkaConsumer010<String>("system.pic_todownload_ali_01", new SimpleStringSchema(), conProp);
        kafkaIn010.setStartFromEarliest();

        SplitStream<EventWithInTime> input = senv.addSource(kafkaIn010)
                .map(new MapFunction<String, EventWithInTime>() {
                    @Override
                    public EventWithInTime map(String value) throws Exception {
                        return new EventWithInTime(System.currentTimeMillis(),value);
                    }
                }).split(new OutputSelector<EventWithInTime>() {
                    @Override
                    public Iterable<String> select(EventWithInTime value) {
                        List<String> res = new ArrayList<>();
                        JSONObject jsonObject = null;
                        try {
                            jsonObject = new JSONObject(value.getValue());
                            if (jsonObject.get("idc").equals("al")){
                                res.add("al");
                            }else if (jsonObject.get("idc").equals("gz")){
                                res.add("gz");
                            }
                        }catch (JSONException exc){
                            LOGGER.error("解析json出错");
                            res.add("json_error");
                            exc.printStackTrace();
                        }
                        return res;
                    }
                });

        DataStream<EventWithInAndOutTime> idc_al = input.select("al")
                .map(new MapFunction<EventWithInTime, EventWithInAndOutTime>() {
                    @Override
                    public EventWithInAndOutTime map(EventWithInTime value) throws Exception {
                        return new EventWithInAndOutTime(value.getIn_time(), System.currentTimeMillis(), value.getValue());
                    }
                });

        DataStream<EventWithInAndOutTime> idc_gz = input.select("gz")
                .map(new MapFunction<EventWithInTime, EventWithInAndOutTime>() {
                    @Override
                    public EventWithInAndOutTime map(EventWithInTime value) throws Exception {
                        return new EventWithInAndOutTime(value.getIn_time(), System.currentTimeMillis(), value.getValue());
                    }
                });

        DataStream<EventWithInAndOutTime> idc_jsonerr = input.select("json_error")
                .map(new MapFunction<EventWithInTime, EventWithInAndOutTime>() {
                    @Override
                    public EventWithInAndOutTime map(EventWithInTime value) throws Exception {
                        return new EventWithInAndOutTime(value.getIn_time(), System.currentTimeMillis(), value.getValue());
                    }
                });


        idc_al.addSink(new BucketingSink<EventWithInAndOutTime>(hdfsDir + "/al").setBucketer(new BasePathBucketer<>()));
        idc_gz.addSink(new BucketingSink<EventWithInAndOutTime>(hdfsDir + "/gz").setBucketer(new BasePathBucketer<>()));
        idc_jsonerr.addSink(new BucketingSink<EventWithInAndOutTime>(hdfsDir + "/json_err").setBucketer(new BasePathBucketer<>()));
        LOGGER.info("flink job will start.");
        senv.execute("splitAndSelect-ver" + versionId);

    }

    public static class EventWithInTime{
        private long in_time;
        private String value;

        public EventWithInTime() {
        }

        public EventWithInTime(long in_time, String value) {
            this.in_time = in_time;
            this.value = value;
        }

        public long getIn_time() {
            return in_time;
        }

        public void setIn_time(long in_time) {
            this.in_time = in_time;
        }

        public String getValue() {
            return value;
        }

        public void setValue(String value) {
            this.value = value;
        }

        @Override
        public String toString() {
            return in_time + "," + value;
        }
    }

    public static class EventWithInAndOutTime extends EventWithInTime{
        private long out_time;

        public EventWithInAndOutTime() {}

        public EventWithInAndOutTime(long in_time, long out_time, String value) {
            super(in_time, value);
            this.out_time = out_time;
        }

        public long getOut_time() {
            return out_time;
        }

        public void setOut_time(long out_time) {
            this.out_time = out_time;
        }

        @Override
        public String toString() {
            return super.getIn_time() + "," +
                    out_time + "," +
                    super.getValue();
        }
    }
}
