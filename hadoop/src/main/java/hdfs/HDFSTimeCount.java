package hdfs;


import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.StringTokenizer;


/**
 * Created by Zhao Qing on 2018/5/16.
 */
public class HDFSTimeCount {
    public static void main(String[] args) throws Exception{
        Configuration conf = new Configuration();//配置信息
        conf.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());//没有这个会报错，报错信息为java.io.IOException: No FileSystem for scheme: hdfs
        Job job = Job.getInstance(conf, "HDFSTimeCount");//创建一个job
        job.setJarByClass(HDFSTimeCount.class);//指定主类
        job.setMapperClass(TimeStampMapper.class);//设置mapper
        job.setCombinerClass(LongSumReducer.class);//设置combiner
        job.setReducerClass(LongSumReducer.class);//设置reducer
        job.setOutputKeyClass(Text.class);//设置输出Key类型
        job.setOutputValueClass(LongWritable.class);//设置输出Value类型
        FileInputFormat.addInputPath(job, new Path(args[0]));//数据源目录（source）
        FileOutputFormat.setOutputPath(job, new Path(args[1]));//目的地目录（sink）
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    public static class TimeStampMapper
            extends Mapper<Object, Text, Text, LongWritable> {//Mapper<输入KEY，输入VALUE,输出KEY,输出VALUE>

        private final static LongWritable one = new LongWritable(1);
        private Text word = new Text();//Text对象是一个UTF-8编码的文本

        /**
         * 每接收到一个<key,value>对，对其执行一次map方法
         * @param key 输入KEY
         * @param value 输入VALUE
         * @param context 输出对象
         * @throws IOException
         * @throws InterruptedException
         */
        public void map(Object key, Text value, Context context
        ) throws IOException, InterruptedException {
            StringTokenizer itr = new StringTokenizer(value.toString(), "\n");//利用输入的value构造一个StringTokenizer对象
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken().split(" ")[0].substring(0,10));//设置键值为时间戳（秒级）
                context.write(word, one);//Context.write(输出KEY,输出VALUE)-->生成一个输出的键值对
            }
        }
    }


    public static class LongSumReducer
            extends Reducer<Text,LongWritable,Text,LongWritable> {//Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
        private LongWritable result = new LongWritable();

        /**
         * This method is called once for each key
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(Text key, Iterable<LongWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            long sum = 0;
            for (LongWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

}
