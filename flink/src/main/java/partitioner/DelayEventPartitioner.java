package partitioner;

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

/**
 * Created by Zhao Qing on 2018/5/24.
 */
public class DelayEventPartitioner<DelayEvent> extends FlinkKafkaPartitioner<DelayEvent> {


    @Override
    public int partition(DelayEvent record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        long sum = 0;
        for (char c:record.toString().toCharArray()
                ) {
            sum += c;
        }
        return partitions[(int) sum % partitions.length];
    }
}
