package mapreduce;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapOutputCollector;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintStream;

/**
 * Create by  ASUS on 2019/9/10.
 * description:
 */
public class WordCount {
    public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
        Configuration conf = new Configuration();
        String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
        
        Job job = new Job();
        job.setJarByClass(WordCount.class);
        job.setJobName("wordcount app");
        
        FileInputFormat.addInputPath(job, new Path(args[0]));
        
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        
        
        job.setMapperClass(WordCountMap.class);
        job.setReducerClass(WordcountReduce.class);
        job.setOutputKeyClass(Text.class);
    
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        System.exit(job.waitForCompletion(true)?0:1);
    }
}

class WordCountMap extends Mapper<LongWritable, Text, Text, IntWritable> {
    private static int count= 0 ;
    public void map(LongWritable key, Text value, Context context) throws          //每行读取
            IOException, InterruptedException {
        PrintStream out = new PrintStream(File.separator+"home"+File.separator+"aidon"+File.separator+"log"+File.separator+"mapr1.log");
        System.setOut(out);
        String line = value.toString();//读取一行数据
        out.println("第"+ ++count + "调用map,text值为:"+line+",key:"+key);
        String str[] = line.split(" ");//因为英文字母是以“ ”为间隔的，因此使用“ ”分隔符将一行数据切成多个单词并存在数组中
        
        for (String s : str) {//循环迭代字符串，将一个单词变成<key,value>形式，及<"hello",1>
            context.write(new Text(s), new IntWritable(1));
        }
    }
}

class WordcountReduce extends Reducer<Text, IntWritable, Text, IntWritable> {
    
    static PrintStream out;
    static {
        try {
            out = new PrintStream(File.separator+"home"+File.separator+"aidon"+File.separator+"log"+File.separator+"mapr2.log");
            System.setOut(out);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
    }
    
    
    private static int rcount = 0;
    public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        out.println("第"+ ++rcount +"次调用reduce，key:"+key+"value位："+values+",centxt伪"+context);
        int count = 0;
        for (IntWritable value : values) {
            count++;
        }
        context.write(key, new IntWritable(count));
    }
}