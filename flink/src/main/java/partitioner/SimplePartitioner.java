package partitioner;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.log4j.Logger;
import org.apache.sling.commons.json.JSONException;
import org.apache.sling.commons.json.JSONObject;

import java.util.Random;

/**
 * Created by Zhao Qing on 2018/5/21.
 */
public class SimplePartitioner<T> extends FlinkKafkaPartitioner<T> {

    private static final Logger LOG = Logger.getLogger(SimplePartitioner.class);
    /**
     * Determine the id of the partition that the record should be written to.
     *
     * @param record the record value
     * @param key serialized key of the record
     * @param value serialized value of the record
     * @param targetTopic target topic for the record
     * @param partitions found partitions for the target topic
     *
     * @return the id of the target partition
     */
    @Override
    public int partition(T record, byte[] key, byte[] value, String targetTopic, int[] partitions){
        Tuple4<Long, Long, String, Integer> rec = (Tuple4<Long, Long, String, Integer>) record;
        JSONObject jsonObject = null;
        String pid = null;
        try {
            jsonObject = new JSONObject(rec.f2);
            pid = jsonObject.get("pid").toString();
        }catch (JSONException exc){
            LOG.warn("pid is null.value: rec.toString()");
            return partitions[new Random().nextInt(partitions.length)];
        }
        int sum = 0;
        for (char c:pid.toCharArray()
             ) {
            sum += c;
        }
        return partitions[sum % partitions.length];
    }
}
