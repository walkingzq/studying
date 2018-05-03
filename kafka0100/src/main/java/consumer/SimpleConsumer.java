package consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Created by Zhao Qing on 2018/5/3.
 */
public class SimpleConsumer {
//    private static final Logger logger = LoggerFactory.getLogger(SimpleConsumer.class);

    public static void main(String[] args){
        String broker = "192.168.188.125:9092";
        String topic = "test010";
        Properties props = new Properties();
        props.put("bootstrap.servers", broker);
        props.put("group.id", "zhaoqing");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
//        consumer.subscribe(Arrays.asList(topic));
        TopicPartition partition = new TopicPartition(topic, 0);
        consumer.assign(Arrays.asList(partition));
        consumer.seek(partition, 11);//消费指定分区指定位置
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records){
                System.out.println(record.offset() + " : " + record.value());
            }

        }
    }
}

