import com.sun.rowset.internal.Row;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaPairRDD$;
import org.apache.spark.api.java.function.*;
import org.slf4j.event.Level;
import scala.Tuple2;
import java.util.*;
import java.util.Random;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.SparkContext;
import java.util.Iterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import java.lang.*;
//import org.apache.log4j.Level;
import org.apache.log4j.Logger;
//import java.util.logging.Logger;
 
import scala.Tuple2;
 
public class sampling_salting
{
 
public static void main(String[]  args)
{
 
 
    Logger.getLogger("org.apache.hadoop").setLevel(org.apache.log4j.Level.WARN);
    Logger.getLogger("org.apache.spark").setLevel(org.apache.log4j.Level.WARN);
    Logger.getLogger("org.project-spark").setLevel(org.apache.log4j.Level.WARN);
 
 
 
    SparkConf conf  = new SparkConf().setMaster("local").setAppName("join");
    JavaSparkContext sc = new JavaSparkContext(conf);
 
 
 
    String path1="hdfs://Desktop:9000/rdd1.csv";
 
    String path2="hdfs://Desktop:9000/rdd2.csv";
 
 
    JavaPairRDD<Integer, String> rdd1 = sc.textFile(path1)
            .mapToPair(new PairFunction<String, Integer, String>()
            {
                @Override
                public Tuple2<Integer, String> call(String s) throws Exception
                {
 
 
                    String[] strings=s.split(",");
 
                    Integer ids = Integer.valueOf(strings[0]);
                    String greet=strings[1];
 
                    return Tuple2.apply(ids,greet);
                }
            });
 
 
 
JavaPairRDD<Integer,String>rdd2=sc.textFile(path2)
.mapToPair(line->{
    String[] strings=line.split(",");
    Integer ids = Integer.valueOf(strings[0]);
    String greet=strings[1];
    return new Tuple2<>(ids,greet);
});
 
    System.out.println(rdd1.collect());
 
    System.out.println(rdd2.collect());
 
 
    JavaPairRDD<Integer, Tuple2<String, String>> result = rdd1.join(rdd2);
    System.out.println(result.collect());
 
 
}
}