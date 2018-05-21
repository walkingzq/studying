package hdfs;

//import org.junit.Test;

import java.util.StringTokenizer;
//
//import static org.junit.Assert.*;

/**
 * Created by Zhao Qing on 2018/5/19.
 */
public class HDFSTimeCountTest {
//    @Test
    public static void main(String[] args){
//        String value = "1526738522807,throughput_testing,4,4,{\"service_id\":\"downloader_pic_hash\",\"download_url\":\"http://10.85.125.52/2018-5-15-22/3/0071bQlVgy1frcdtmiv14j30qo0qoq86.jpg\",\"idc\":\"\",\"pid\":\"0071bQlVgy1frcdtmiv14j30qo0qoq86\"}";
        String value = "1526700293637,throughput_testing,4,4,{\"service_id\":\"downloader\",\"download_url\":\"http://10.85.136.210/2018-5-15-16/2/0075fLshly1frc470lqvfj30u01hck10.jpg\",\"idc\":\"al\",\"pid\":\"0075fLshly1frc470lqvfj30u01hck10\"}\n" +
                "1526700293637,throughput_testing,4,4,{\"service_id\":\"downloader\",\"download_url\":\"http://10.85.136.200/2018-5-15-16/9/be2c857agy1frc463vmcyj20zi0qogto.jpg\",\"idc\":\"gz\",\"pid\":\"be2c857agy1frc463vmcyj20zi0qogto\"}\n";
        StringTokenizer itr = new StringTokenizer(value.toString(), "\n");//利用输入的value构造一个StringTokenizer对象
        while (itr.hasMoreTokens()) {
            System.out.println(itr.nextToken().split(" ")[0].substring(0,10));//设置键值为时间戳（秒级）
//            context.write(word, one);//Context.write(输出KEY,输出VALUE)-->生成一个输出的键值对
        }
    }

}