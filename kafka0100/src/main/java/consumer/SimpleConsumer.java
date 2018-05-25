package consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;

import java.util.Arrays;
import java.util.Properties;

/**
 * Created by Zhao Qing on 2018/5/3.
 * consumer.SimpleConsumer
 */
public class SimpleConsumer {

    public static void main(String[] args){
        String topic = "delayTesting01";//topic名称
        Properties props = new Properties();
        props.put("bootstrap.servers", "10.87.52.135:9092,10.87.52.134:9092,10.87.52.158:9092");
        props.put("group.id", "zhaoqing");//group.id
        props.put("enable.auto.commit", "true");//自动提交offset
        props.put("auto.commit.interval.ms", "1000");//自动提交offset间隔
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");//key序列化
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");//value序列化
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);//创建一个KafkaConsumer对象
        consumer.subscribe(Arrays.asList(topic));//订阅topic
        //指定offset的粒度是分区，因此如果该consumer有多个分区的话，需要手动指定多个分区
        //注意：同一个consumer不能同时通过subscribe和assign方法指定topic和分区
//        TopicPartition partition = new TopicPartition(topic, 0);
//        consumer.assign(Arrays.asList(partition));
//        consumer.seek(partition, 11);//消费指定分区指定位置
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(100);
            for (ConsumerRecord<String, String> record : records){
                System.out.println(record.timestamp() + "," + record.value());
            }
        }
    }
}

