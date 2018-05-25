package partitioner;

import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;

/**
 * Created by Zhao Qing on 2018/5/21.
 */
public class StringPartitioner<T> extends FlinkKafkaPartitioner<T>{

    @Override
    public int partition(T record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
        String str =  record.toString();
        long sum = 0;
        for (char c:str.toCharArray()
             ) {
            sum += c;
        }
        return partitions[(int) sum % partitions.length];
    }

}
