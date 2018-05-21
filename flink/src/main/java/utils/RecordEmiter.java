package utils;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Created by Zhao Qing on 2018/5/14.
 */
public class RecordEmiter {

    public static void main(String[] args){
        RecordEmiter emiter = new RecordEmiter();
        emiter.job(100000);
    }

    // 要求100的倍数
    public void job(int rate) {
// 规定10ms执行一次，再短就太不精确了。总共执行1000ms。
        int duration = 1000;
        int period = 10;
        int times = duration / period;

        int limit = rate / times; // 10ms内应该执行多少次

        ScheduledExecutorService scheduledThreadPool = Executors.newScheduledThreadPool(2);// 单线程
        AtomicInteger count = new AtomicInteger(0);// 计数器
        Runnable command = () -> {
            for (int i = 0; i < limit; i++) {
                int no = count.incrementAndGet();
                if (no > rate) { // 判断是
                    break;
                }
                System.out.println("hehe: " + (no - 1));
            }
        };

        long start = System.currentTimeMillis();
// 执行次数，duration/period+1！所以需要限制 - 见下面的awaitTermination及shutdownNow
        scheduledThreadPool.scheduleAtFixedRate(command, 0, period, TimeUnit.MILLISECONDS);
        try {
            scheduledThreadPool.awaitTermination(duration, TimeUnit.MILLISECONDS);// 如果都执行完了，就不会等待！
// scheduledThreadPool.shutdownNow();
            scheduledThreadPool.shutdown();

            long end = System.currentTimeMillis();
            System.out.println(start);
            System.out.println(end);
            System.out.println(end - start);

        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
