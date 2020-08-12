import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;
import java.util.*;
import org.apache.spark.SparkConf;
import java.util.Iterator;
import java.util.Random;
import org.apache.spark.api.java.function.FlatMapFunction;

public class salting
{

//    运行办法:
//    ①启动hadoop
//    ②启动Spark
//    ③先运行maven
//    ④再在intellij中修改maven生成的jar包的路径，然后运行
//------------------------------下面是顶层函数------------------------------------------------------------



    public static void main(String[] args)
    {

        SparkConf conf = new SparkConf().setMaster("local").setAppName("wordcount1");
        JavaSparkContext sc = new JavaSparkContext(conf);
        sc.setLogLevel("WARN");

//--------------------------下面开始是wordcount----------------------------






        String filename = "/home/appleyuchi/桌面/spark_success/wordcount/Java/src/main/java/hello.txt";
        JavaRDD<String> input = sc.textFile(filename);

        JavaRDD<String> lines = input.flatMap(new FlatMapFunction<String, String>()
                                              {

                                                  public Iterator<String> call(String s) throws Exception

                                                  {
                                                      return Arrays.asList(s.split(" ")).iterator();
                                                  }
                                              }
        );

        System.out.print("----------------------lines-------------------------------\n");
        System.out.println("lines="+lines.collect());


        //pairs
        JavaPairRDD<String,Long> pairs = lines.mapToPair(new PairFunction<String, String,Long>()
        {
            public Tuple2<String, Long> call(String s) throws Exception
            {
                return new Tuple2<String, Long>(s,1L);
            }
        });

        System.out.print("----------------------paris-------------------------------\n");

        System.out.println("paris="+pairs.collect());

       //reduce
       JavaPairRDD<String,Long> counts = pairs.reduceByKey(new Function2<Long,Long,Long>()
       {
           public Long call(Long x, Long y) throws Exception
           {
               return x+y;
           }
       });


       System.out.print("----------------------counts-------------------------------\n");
       System.out.println("counts="+counts.collect());





    }
}





