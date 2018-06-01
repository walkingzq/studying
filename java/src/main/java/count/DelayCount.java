package count;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 * Created by Zhao Qing on 2018/5/25.
 * 延时统计
 */
public class DelayCount {
    public static void main(String[] args) throws Exception{
        String fileName = "F:\\个人\\微博\\实时流框架测试\\flink测试结果\\splitAndSelect\\total-counts.csv";
        try (FileReader fr = new FileReader(fileName)){
            long count = 0;
            long sumDelay = 0;
            BufferedReader br = new BufferedReader(fr);
            String line = null;
            int err1Count = 0, err2Count = 0;
            while ((line = br.readLine()) != null){
                long delay = 0;
                try {
                    delay = Long.parseLong(line.split("\\s")[1]);
                }catch (NumberFormatException exc){
                    err1Count++;
                    System.err.println("数字转换错误");
                    exc.printStackTrace();
                    continue;
                }catch (ArrayIndexOutOfBoundsException exc){
                    err2Count++;
                    System.err.println("数组越界");
                    exc.printStackTrace();
                    continue;
                }
                count++;
                System.out.println(delay);
                sumDelay += delay;
            }
            System.out.println("记录数：" + count + ",平均延时：" + sumDelay / count);
            System.out.println("转换错误:" + err1Count + ",数组越界：" + err2Count);
        }catch (FileNotFoundException exc){
            throw exc;
        }catch (IOException exc){
            throw  exc;
        }
    }
}
