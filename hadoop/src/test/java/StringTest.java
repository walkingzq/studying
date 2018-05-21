//import org.junit.Test;

import java.util.StringTokenizer;

/**
 * Created by Zhao Qing on 2018/5/16.
 */
public class StringTest {
//    @Test
    public void StringTokeTest(){
        String str = "1526455919523 throughput_testing_01 {\"service_id\":\"downloader_pic_hash\",\"download_url\":\"http://10.85.125.52/2018-5-15-17/2/006CtLKUly1frc6awpid3j30j60pkq51.jpg\",\"idc\":\"\",\"pid\":\"006CtLKUly1frc6awpid3j30j60pkq51\"}\n" +
                "1526455919523 throughput_testing_01 {\"service_id\":\"downloader_pic_hash\",\"download_url\":\"http://10.85.125.51/2018-5-15-17/8/005xgL06gy1frc6bun4dtj30ha0jrqdt.jpg\",\"idc\":\"\",\"pid\":\"005xgL06gy1frc6bun4dtj30ha0jrqdt\"}";
        StringTokenizer st = new StringTokenizer(str,"\n");
        while (st.hasMoreElements()){
            System.out.println(st.nextElement());
            System.out.println();
        }
    }
}
