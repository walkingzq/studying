import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer011;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchemaWrapper;

import java.util.Properties;

/**
 * Created by Zhao Qing on 2018/5/18.
 */
public class Kafka011 {
    public static void main(String[] args){
        FlinkKafkaProducer011 kafkaProducer011 = new FlinkKafkaProducer011("", new KeyedSerializationSchemaWrapper<>(new SimpleStringSchema()), new Properties(), FlinkKafkaProducer011.Semantic.EXACTLY_ONCE);
    }
}
