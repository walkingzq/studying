package count;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileReader;

/**
 * Created by Zhao Qing on 2018/5/24.
 */
public class ThroughputCount {
    public static void main(String[] args) throws Exception{
        int[] taskManagerNums = new int[]{1,4};
        int[] taskslotPerTaskManager = new int[]{1,4,16};
        String fileNamePre = "F:\\个人\\微博\\实时流框架测试\\flink测试结果\\吞吐量测试\\直接输入输出场景\\throughput_";
        for (int tm_num:taskManagerNums
             ) {
            for (int ts_num:taskslotPerTaskManager
                 ) {
                long sum = 0;
                int count = 0;
                try (FileReader fr = new FileReader(fileNamePre + tm_num + "_" + ts_num + ".csv");){
                    BufferedReader br = new BufferedReader(fr);
                    String line = null;
                    while ((line = br.readLine()) != null){
                        count++;
//                        System.out.println(line);
//                        System.out.println(line.split("\\s")[0]);
                        sum += Long.parseLong(line.split("\\s")[1]);
                    }
                    System.out.println("===============================tmNUm:" + tm_num + "," + "tsNum" + ts_num + "==============================");
                    System.out.println("count:" + count + ",平均吞吐量:" + sum / count);
                }catch (FileNotFoundException exc){
                    throw exc;
                }

            }
        }

    }
}
