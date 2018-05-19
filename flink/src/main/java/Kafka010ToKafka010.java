import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.bucketing.BasePathBucketer;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.log4j.Logger;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Created by Zhao Qing on 2018/5/15.
 */
public class Kafka010ToKafka010 {
    private static Logger LOGGER = Logger.getLogger(Kafka010ToKafka010.class);

    public static void main(String[] args) throws Exception{
        final StreamExecutionEnvironment senv = StreamExecutionEnvironment.getExecutionEnvironment();
        senv.enableCheckpointing(1000);//启动checkpoint，间隔1ms
        senv.setStateBackend(new FsStateBackend("hdfs://emr-header-1/flink/checkpoints_zq"));//设置stateBackend
        senv.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)));//设置重启策略，如果任务失败，会依据重启策略进行后续动作
        //默认情况下，一个flink作业的checkpoints将在job cancel时自动删除。如果想要保留这些checkpoints，按照如下两行设置即可。
        //注：设置为RETAIN_ON_CANCELLATION后flink不会自动删除checkpoints，如果不需要这些checkpoint是时需要手动删除
//        CheckpointConfig checkpointConfig = senv.getCheckpointConfig();
//        checkpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);

        //输入kafka信息（kafka010版本）
        Properties prop = new Properties();
        prop.setProperty("bootstrap.servers", "10.87.52.135:9092,10.87.52.134:9092,10.87.52.158:9092");
//        prop.setProperty("zookeeper.connect", "10.87.52.135:2181,10.87.52.134:2181,10.87.52.158:2181/kafka-0.10.1.1");
        prop.setProperty("group.id", "img_copy_to_self");
        FlinkKafkaConsumer010<String> kafkaIn010 = new FlinkKafkaConsumer010<String>("system.pic_todownload_ali_01", new SimpleStringSchema(), prop);
        kafkaIn010.setStartFromGroupOffsets();

        //输出kafka信息
        String broker = "10.87.52.135:9092,10.87.52.134:9092,10.87.52.158:9092";
        String producerTopic = "system.pic_todownload_ali_01";
        FlinkKafkaProducer010<String> kafkaOut010 = new FlinkKafkaProducer010<>(broker, producerTopic, new SimpleStringSchema());
        //以下两项设置外加checkpoint机制保证kafka producer为at least once语义
        //默认false，如果设置为true，在遇到写kafka错误时，flink仅仅会记录出错信息，而不会抛出异常并停止运行
        kafkaOut010.setLogFailuresOnly(false);
        //默认false，如果设置为true，flink将会在checkpoint时等待kafka确认在该时刻正在传输的记录。at least once语义
        kafkaOut010.setFlushOnCheckpoint(true);

        Kafka010ToKafka010 kafka010ToKafka010 = new Kafka010ToKafka010();
        LOGGER.info("starting...");
        kafka010ToKafka010.run(senv, kafkaIn010, kafkaOut010);
    }


    /**
     *
     * @param senv
     * @param kafkaIn010
     * @param kafkaOut010
     * @throws Exception
     */
    public void run(StreamExecutionEnvironment senv, FlinkKafkaConsumer010<String> kafkaIn010, FlinkKafkaProducer010<String> kafkaOut010) throws Exception{
        DataStream<String> img_copy_to_self = senv.addSource(kafkaIn010);
        img_copy_to_self.addSink(kafkaOut010);
        img_copy_to_self.addSink(new BucketingSink<String>("hdfs://emr-header-1/home/flink/flink_test_zq/img2").setBucketer(new BasePathBucketer<>()));
        senv.execute("img_copy_to_self");
    }
}
