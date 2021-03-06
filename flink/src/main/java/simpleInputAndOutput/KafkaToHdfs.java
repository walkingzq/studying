package simpleInputAndOutput;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.fs.bucketing.BasePathBucketer;
import org.apache.flink.streaming.connectors.fs.bucketing.BucketingSink;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

/**
 * Created by Zhao Qing on 2018/5/4.
 * 数据流向：kafka --flink--> hdfs
 */
public class KafkaToHdfs {
    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaToHdfs.class);

    /**
     * flink run -m yarn-cluster -yn 4 -yjm 1024 -ytm 4096 [servers] [zk] [groupId] [topic] [hdfsPath]
     * 所需参数包括：
     * servers kafka-broker-servers
     * zk kafka-zk
     * groupId consumer-groupId
     * topic 要消费的topic名称
     * hdfsPath 要输出的hdfs基础路径
     * @usage java -cp [jar_path] simpleInputAndOutput.KafkaToHdfs [servers] [zk] [groupId] [topic] [hdfsPath]
     * @example java -cp [jar_path] simpleInputAndOutput.KafkaToHdfs 10.87.52.135:9092,10.87.52.134:9092,10.87.52.158:9092 10.87.52.135:2181,10.87.52.134:2181,10.87.52.158:2181/kafka-0.10.1.1 kafkaToHdfs test hdfs://emr-header-1/home/flink/flink_test_zq
     * flink run -m yarn-cluster -yn 4 -yjm 1024 -ytm 4096 flink-1.0-SNAPSHOT-jar-with-dependencies.jar 10.87.52.135:9092,10.87.52.134:9092,10.87.52.158:9092 10.87.52.135:2181,10.87.52.134:2181,10.87.52.158:2181/kafka-0.10.1.1 kafkaToHdfs test hdfs://emr-header-1/home/flink/flink_test_zq/data
     * @param args
     * @throws Exception
     */
    public static void main(String[] args) throws Exception{
        String servers = args[0];
        String zk = args[1];
        String groupId = args[2];
        String topic = args[3];
        String hdfsPath = args[4];
        LOGGER.info("servers:{},zk:{},groupId:{},topic:{},hdfsPath:{}",servers,zk,groupId,topic,hdfsPath);
//        String servers = "10.87.52.135:9092,10.87.52.134:9092,10.87.52.158:9092";
//        String zk = "10.87.52.135:2181,10.87.52.134:2181,10.87.52.158:2181/kafka-0.10.1.1";
//        String groupId = "kafkaToHdfs";
//        String topic = "test";
//        String hdfsPath = "hdfs://emr-header-1/home/flink/flink_test_zq";
        KafkaToHdfs kafkaToHdfs = new KafkaToHdfs();
        LOGGER.info("running...");
        kafkaToHdfs.run(servers, zk, groupId, topic, hdfsPath);
    }

    public void run(String servers, String zk, String groupId, String topic, String hdfsPath) throws Exception{
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", servers);
        properties.setProperty("zookeeper.connect", zk);
        properties.setProperty("group.id", groupId);
        LOGGER.info(properties.toString());
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();//获取flink运行环境
//        env.setStreamTimeCharacteristic(TimeCharacteristic.IngestionTime);//设置时间格式为IngestionTime
        env.enableCheckpointing(5000);//flink checkpoint 间隔，5000ms
        //创建一个kafka消费者，注意flink支持topic的正则表达式（即可以根据指定规则自动发现kafka topic并进行消费，默认offset为最早）
        FlinkKafkaConsumer010<String> kafkaConsumer010 = new FlinkKafkaConsumer010<String>(topic, new SimpleStringSchema(), properties);
        kafkaConsumer010.setStartFromEarliest();//设定consumer的offset为最早//TODO:后期可以修改为从命令行指定offset
        DataStream<String> stream = env.addSource(kafkaConsumer010);//创建一个flink DataStream
        BucketingSink<String> hdfs = new BucketingSink<>(hdfsPath);
        //flink支持目录回滚机制，通过BucketingSink.setBucketer(Bucketer<T> bucketer)可以指定输出数据所在子目录的划分方式
        //flink原生提供了BasePathBucketer和DateTimeBucketer:
        //1.BasePathBucketer方式是不划分子目录，将所有都输出至给定的hdfs-path;
        //2.DateTimeBucketer可以按照时间周期（SimpleDateFormat格式）划分子目录
        hdfs.setBucketer(new BasePathBucketer<>());
        stream.addSink(hdfs);//添加hdfs-sink
        try {
            LOGGER.info("starting...");
            env.execute("kafka-to-hdfs");//开始执行
        }catch (Exception exc){
            LOGGER.error("[error]:{}",exc.getMessage());
            throw exc;
        }
    }
}
