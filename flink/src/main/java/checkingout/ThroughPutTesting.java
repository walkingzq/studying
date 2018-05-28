package checkingout;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.Clock;
import org.apache.flink.streaming.connectors.fs.StringWriter;
import org.apache.flink.streaming.connectors.fs.bucketing.Bucketer;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.hadoop.fs.Path;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Created by Zhao Qing on 2018/5/16.
 * checkingout.ThroughPutTesting
 * 内容：吞吐量测试
 * 思路：接入一个无限量数据（相对而言）的kafka topic作为flink输入，flink在收到数据后添加一个时间戳就输出至另一个kafka topic和hdfs
 * 运行：
 * flink run -m yarn-cluster -yn 4 -ys 1 -yjm 1024 -ytm 4096 [jar_name] [taskManager_num] [taskSlotPerTaskManager_num] [hdfsPath] [isEnableCheckpoint] [checkpointInterval]
 */
public class ThroughPutTesting {
    public static void main(String[] args) throws Exception{
        //参数读入
        String taskManager_num = args[0];
        String taskSlotPerTaskManager_num = args[1];
        String hdfsPath = "hdfs://emr-header-1" + args[2];
        boolean isEnableCheckpoint = Byte.parseByte(args[3]) == 1;//是否开启checkpoint
        int checkpointInterval = 5000;//checkpoint间隔，默认5000ms
        if (isEnableCheckpoint){checkpointInterval = Integer.parseInt(args[4]);}
        //flink运行环境设置
        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);//设置时间格式为IngestionTime,即进入flink时间
        if (isEnableCheckpoint){
            senv.enableCheckpointing(checkpointInterval);
            senv.setStateBackend(new FsStateBackend("hdfs://emr-header-1/flink/checkpoints_zq"));//设置stateBackend
            senv.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));//重启策略
        }

        //输入kafka信息（kafka010版本）
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "10.87.52.135:9092,10.87.52.134:9092,10.87.52.158:9092");
        prop.setProperty("group.id", "throughput_testing");
        FlinkKafkaConsumer010<String> kafkaIn010 = new FlinkKafkaConsumer010<String>("system.pic_todownload_ali_01", new SimpleStringSchema(), prop);
        kafkaIn010.setStartFromEarliest();//从最早开始读取

        //输出kafka信息（kafka010版本）
        String broker = "10.87.52.135:9092,10.87.52.134:9092,10.87.52.158:9092";
        String producerTopic = "basetest";
        FlinkKafkaProducer010<String> kafkaOut010 = new FlinkKafkaProducer010<>(broker, producerTopic, new SimpleStringSchema());
        kafkaOut010.setWriteTimestampToKafka(true);//将进入flink时间作为kafka记录的时间戳
        if (isEnableCheckpoint){
            kafkaOut010.setFlushOnCheckpoint(true);
        }


        //输出kafka信息（kafka010版本）
//        String producerTopic2 = "throughput.testing.noFlinkTime";
        String producerTopic2 = "topic2";
        FlinkKafkaProducer010<String> kafkaOut010_02 = new FlinkKafkaProducer010<>(broker, producerTopic2, new SimpleStringSchema());
        if (isEnableCheckpoint){
            kafkaOut010_02.setFlushOnCheckpoint(true);
        }


        //dataflow配置
        DataStream<String> stream = senv.addSource(kafkaIn010)
                .map(new MapFunction<String, String>() {//添加标志
            @Override
            public String map(String value) throws Exception {
                String delimer = ",";
                StringBuffer sb = new StringBuffer();
                sb.append("throughput_testing").append(delimer).
                        append(taskManager_num).append(delimer).
                        append(taskSlotPerTaskManager_num).append(delimer).append(value);
                return sb.toString();
            }
        }).map(new MapFunction<String, String>() {//添加时间戳
            @Override
            public String map(String value) throws Exception {
                String delimer = ",";
                StringBuffer sb = new StringBuffer();
                sb.append(System.currentTimeMillis()).append(delimer).append(value);
                return sb.toString();
            }
        });
        BucketingSink hdfsSink = new BucketingSink<String>(hdfsPath)
                .setBucketer(new SelfBucketer<>());
//        hdfsSink.setWriter(new StringWriter());//设置writer
//        hdfsSink.setInactiveBucketCheckInterval();
//        hdfsSink.setInactiveBucketThreshold();
//        hdfsSink.setBatchSize(400 * 1024 * 1024);//设置每个文件的大小
        stream.addSink(hdfsSink);
//        stream.addSink(kafkaOut010);
//        stream.addSink(kafkaOut010_02);
        //开始执行
        senv.execute("ThroughPutTesting");
    }


    public static class SelfBucketer<T> implements Bucketer<T>{
        public Path getBucketPath(Clock clock, Path basePath, T element){
            StringBuffer sb = new StringBuffer(basePath.toString()).append("/");
            String[] str = element.toString().split(",");
            sb.append(str[2]).append("/").append(str[3]).append("/data");
            Path bucketPath = new Path(sb.toString());
            return bucketPath;
        }
    }
}


