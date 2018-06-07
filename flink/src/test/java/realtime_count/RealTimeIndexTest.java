package realtime_count;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

/**
 * Created by Zhao Qing on 2018/6/7.
 */
public class RealTimeIndexTest {
    @Test
    public void addIndex() throws Exception {
        Map<String, Long> indexs = new HashMap<>();
        indexs.put("fwd",(long) 12);

        Map<String, Long> indexsToAdd = new HashMap<>();
        indexsToAdd.put("cmt", (long) 14);
        indexsToAdd.put("lk", (long) 15);

        SimpleDemo.RealTimeIndex realTimeIndex = new SimpleDemo.RealTimeIndex(System.currentTimeMillis() / 1000, System.currentTimeMillis() / 1000, indexs);

        System.out.println(realTimeIndex.toJson());

        realTimeIndex.addIndex(indexsToAdd);
        System.out.println(realTimeIndex.toJson());
    }

}