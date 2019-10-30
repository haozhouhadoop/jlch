package mapr;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.List;
import java.util.function.Function;

/**
 * Create by  ASUS on 2019/9/10.
 * description:
 */
public class test {
    public static void main(String[] args) throws FileNotFoundException {
        
        String appName = "";
        SparkConf conf = new SparkConf().setAppName(appName);
        JavaSparkContext sc = new JavaSparkContext(conf);
        
        JavaRDD<String> distFile = sc.textFile("hello");
        List<Integer> data = Arrays.asList(1,2,3,4,5);
        JavaRDD<Integer> disData = sc.parallelize(data);
        
        disData.reduce()
      
    }
}

class A{
    static int count = 0;
    
    void say(){
        System.out.println("count="+ ++count);
    }
}
