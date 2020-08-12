//import org.apache.spark.api.java.JavaRDD;
//import org.apache.spark.api.java.JavaPairRDD;
//import org.apache.spark.api.java.JavaSparkContext;
//import org.apache.spark.api.java.function.*;
//import scala.Tuple2;
//import java.util.*;
//import org.apache.spark.SparkConf;
//
//import javax.swing.*;
//import java.util.Iterator;
//import java.util.Random;
//
//public class javaread {
//
//
//
//// -----------------------------------读取数据-----------------------------
//    public static void main(String[]  args)
//    {
//        SparkConf conf = new SparkConf().setMaster("spark://Desktop:7077").setJars(new String[]{"/home/appleyuchi/桌面/spark_success/Spark数据倾斜处理/Java/sampling_salting/target/sampling-salting-1.0-SNAPSHOT.jar"}).setAppName("TestSpark");
//        JavaSparkContext sc = new JavaSparkContext(conf);
//        sc.setLogLevel("WARN");
//        final JavaRDD<String>lines = sc.textFile("hdfs://Desktop:9000/rdd1.csv");
//
//
//
//        System.out.println(lines.collect());
//
//
////
////        final String header= lines.first();
////        JavaRDD<String> lines2 = (JavaRDD<String>) lines.filter
////                (
////                new Function<String, Boolean>()
////                {
////                    private static final long serialVersionUID = 1L;
////                    @Override
////            public Boolean call(String v1) throws Exception
////                    {
////                return !v1.equals(header);
////            }
////        });
////
////        System.out.println(lines2.collect());
//
//    }
//
//
//
//}
