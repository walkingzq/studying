package examples;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
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
public class WordCount {
    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, IntWritable> {//Mapper<输入KEY，输入VALUE,输出KEY,输出VALUE>

        private final static IntWritable one = new IntWritable(1);
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
            StringTokenizer itr = new StringTokenizer(value.toString());//利用输入的value构造一个StringTokenizer对象
            while (itr.hasMoreTokens()) {
                word.set(itr.nextToken());
                context.write(word, one);//Context.write(输出KEY,输出VALUE)-->生成一个输出的键值对
            }
        }
    }


    public static class IntSumReducer
            extends Reducer<Text,IntWritable,Text,IntWritable> {//Reducer<KEYIN,VALUEIN,KEYOUT,VALUEOUT>
        private IntWritable result = new IntWritable();

        /**
         * This method is called once for each key
         * @param key
         * @param values
         * @param context
         * @throws IOException
         * @throws InterruptedException
         */
        public void reduce(Text key, Iterable<IntWritable> values,
                           Context context
        ) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable val : values) {
                sum += val.get();
            }
            result.set(sum);
            context.write(key, result);
        }
    }

    /**
     * 程序主入口
     * @param args
     * 需要输入的参数有两个：数据源目录和目的地目录
     * @throws Exception
     */
    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();//配置信息
        Job job = Job.getInstance(conf, "word count");//创建一个job
        job.setJarByClass(WordCount.class);//指定主类
        job.setMapperClass(TokenizerMapper.class);//设置mapper
        job.setCombinerClass(IntSumReducer.class);//设置combiner
        job.setReducerClass(IntSumReducer.class);//设置reducer
        job.setOutputKeyClass(Text.class);//设置输出Key类型
        job.setOutputValueClass(IntWritable.class);//设置输出Value类型
        FileInputFormat.addInputPath(job, new Path(args[0]));//数据源目录（source）
        FileOutputFormat.setOutputPath(job, new Path(args[1]));//目的地目录（sink）
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
