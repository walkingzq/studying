package utils;


import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.sling.commons.json.JSONObject;

/**
 * Created by Zhao Qing on 2018/5/14.
 */
public class RecordEmiterTest {

    public void job() throws Exception {
        new RecordEmiter().job(100);
    }

    public static void main(String[] args) throws Exception{
//        String str = "{\"service_id\":\"downloader_pic_hash\",\"download_url\":\"http://10.85.125.53/2018-5-15-18/6/006eJb74gy1frc6pi4ggpj33k02o0hdv.jpg\",\"idc\":\"\",\"pid\":\"006eJb74gy1frc6pi4ggpj33k02o0hdv\"}";
        String str = "{\"service_id\":\"downloader\",\"download_url\":\"http://10.85.136.197/2018-5-15-18/9/006Q4REegy1frc7ov0fxjj30m80m8mz0.jpg\",\"idc\":\"gz\",\"pid\":\"006Q4REegy1frc7ov0fxjj30m80m8mz0\"}";
//        Tuple4<Long, Long, String, Integer> record = new Tuple4<>(System.currentTimeMillis(), System.currentTimeMillis(), str, 1);
//        JSONObject jsonObject = new JSONObject(record.f2);
//        System.out.println(jsonObject.get("pid"));
//        long sum = 0;
//        for (char c : jsonObject.get("pid").toString().toCharArray()){
//            sum += c;
//        }
//        System.out.println(sum);
//        System.out.println(sum % 20);
        String str0 = System.currentTimeMillis() + "," + str;
        System.out.println(str0.substring(0,13));
        System.out.println(str0.substring(14));
    }

}