package realtime_count;

//import org.junit.Test;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.deser.DataFormatReaders;

import java.awt.*;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

//import static org.junit.Assert.*;

/**
 * Created by Zhao Qing on 2018/6/7.
 */
public class RealTimeIndexTest {
//    @Test
    public static void addIndex() throws Exception {
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

    public static void main(String[] args) throws Exception{
        String str = "{\"start_time\":\"2018-06-09 16:20:00\",\"end_time\":\"2018-06-09 16:29:59\",\"indexs\":{\"lk_5_4\":\"16\",\"other\":\"1215732\",\"cmt_5_4\":\"9\",\"lk_5_3\":\"13\",\"lk_5_2\":\"13\",\"cmt_5_3\":\"8\",\"cmt_5_2\":\"4\",\"cmt_5_1\":\"11\",\"cmt_5_0\":\"10\",\"lk_5_1\":\"16\",\"lk_5_0\":\"50\"}}";
        Pattern p = Pattern.compile("\\{\"start_time\":\"(\\d{4}-\\d{2}-\\d{2} \\d{2}:\\d{2}:\\d{2})");
        Matcher m = p.matcher(str);
        if (m.find()){
            System.out.println(m.group(1));
        }else {
            System.out.println("not find.");
        }
    }

    public static long fixedWindow(){
        long timestamp = 1528411199000L;
        String str = String.valueOf(timestamp);
        String pre = str.substring(0,6);
        int time = Integer.parseInt(str.substring(6,10));
        int i = 599;
        while (time > i){
            i += 600;
        }
        String res = "0000";
        if (i > 599){
            res = String.valueOf(i - 599);
        }
        while (res.length() < 4){
            res = "0" + res;
        }
        return Long.parseLong(pre + res + "000");
    }


}