package hdfs;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;

/**
 * Created by Zhao Qing on 2018/5/25.
 * hdfs.DelayCount
 */
public class DelayCount {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();//配置信息
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());//没有这个会报错，报错信息为java.io.IOException: No FileSystem for scheme: hdfs
        Job job = Job.getInstance(conf, "DelayCount");//创建一个job
        job.setJarByClass(HDFSTimeCount.class);//指定主类
        job.setMapperClass(DelayMapper.class);//设置mapper
//        job.setCombinerClass(LongSumReducer.class);//设置combiner
//        job.setReducerClass(LongSumReducer.class);//设置reducer
        job.setOutputKeyClass(Text.class);//设置输出Key类型
        job.setOutputValueClass(LongWritable.class);//设置输出Value类型
        FileInputFormat.addInputPath(job, new Path(args[0]));//数据源目录（source）
        FileOutputFormat.setOutputPath(job, new Path(args[1]));//目的地目录（sink）
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    private static class DelayMapper extends Mapper<Object, Text, Text, LongWritable>{

        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");//利用输入的value构造一个StringTokenizer对象
            while (itr.hasMoreTokens()) {
                String[] strs = itr.nextToken().split(",");
                long in_time = Long.parseLong(strs[0]);
                long out_time = Long.parseLong(strs[1]);
                context.write(value, new LongWritable(out_time - in_time));//Context.write(输出KEY,输出VALUE)-->生成一个输出的键值对
            }
        }
    }


}
