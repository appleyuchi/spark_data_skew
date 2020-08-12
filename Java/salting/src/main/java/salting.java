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
//-------------------------------下面是被调用的函数----------------------------------------------------------

// 第一步，给RDD中的每个key都打上一个随机前缀。
    private static   PairFunction<Tuple2<String,Long>,String,Long> func1 = new PairFunction <Tuple2<String,Long>, String, Long>()
    {private static final long serialVersionUID = 1L;


        public Tuple2<String, Long>call(Tuple2<String, Long> lalala)throws Exception //这个地方使用了函数模板，在java中称为范型
        {
            Random random = new Random();
            int prefix = random.nextInt(10);
            return new Tuple2<String, Long>(prefix + "_" + lalala._1, lalala._2);
        }
    };


// 第二步，对打上随机前缀的key进行局部聚合。
private static Function2 <Long, Long,Long> func2=new Function2<Long,Long,Long>()
{ private static final long serialVersionUID = 1L;

    public Long call(Long v1, Long v2) throws Exception
    {
        return v1+v2;
    }

};





// 第三步，去除RDD中每个key的随机前缀。

private static PairFunction <Tuple2<String,Long>, String, Long> func3=new PairFunction<Tuple2<String,Long>, String, Long>()
{
    private static final long serialVersionUID = 1L;

    public Tuple2<String, Long> call(Tuple2<String, Long> tuple)throws Exception
    {
        String originalKey;
        originalKey = String.valueOf(tuple._1.split("_")[1]);
        return new Tuple2<String, Long>(originalKey, tuple._2);
    }
};


// 第四步，对去除了随机前缀的RDD进行全局聚合。
    private static Function2<Long, Long, Long> func4=new Function2<Long, Long, Long>()
    {private static final long serialVersionUID = 1L;

    public Long call(Long v1, Long v2) throws Exception {
        return v1 + v2;
    }
    };

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

//        //reduce
//        JavaPairRDD<String,Long> counts = pairs.reduceByKey(new Function2<Long,Long,Long>()
//        {
//            public Long call(Long x, Long y) throws Exception
//            {
//                return x+y;
//            }
//        });
//
//
//        System.out.print("----------------------counts-------------------------------\n");
//        System.out.println("counts="+counts.collect());





//-------------------------下面开始是spark的加盐----------------------------
// 第一步，给RDD中的每个key都打上一个随机前缀。
        JavaPairRDD<String,Long> randomPrefixRdd = pairs.mapToPair(func1);
        System.out.println("-----------第1步---randomPrefixRdd----------------------");
        System.out.print(randomPrefixRdd.collect());
        System.out.println();




// 第二步，对打上随机前缀的key进行局部聚合。
        JavaPairRDD<String, Long> localAggrRdd = randomPrefixRdd.reduceByKey(func2);
//        JavaPairRDD<Long, Long> localAggrRdd = randomPrefixRdd.reduceByKey(func2);
        System.out.println("------------第2步-----localAggrRdd ----------------------");
        System.out.print(localAggrRdd.collect());
        System.out.println();


//// 第三步，去除RDD中每个key的随机前缀。
        JavaPairRDD<String, Long> removedRandomPrefixRdd = localAggrRdd.mapToPair(func3);
        System.out.println("----------第3步--lremovedRandomPrefixRdd ----------------------\n");
        System.out.println(removedRandomPrefixRdd.collect());




//// 第四步，对去除了随机前缀的RDD进行全局聚合。
        JavaPairRDD<String, Long> globalAggrRdd = removedRandomPrefixRdd.reduceByKey(func4);
        System.out.println("-----------第4步---globalAggrRdd----------------------");
        System.out.println(globalAggrRdd.collect());







    }
}





