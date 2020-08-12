import com.sun.rowset.internal.Row;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import scala.Tuple2;
import java.util.*;
import java.util.Random;
import java.util.Iterator;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import java.lang.*;
import org.apache.log4j.Logger;


public class section7
{
    public static void main(String[]  args)
    {

//消除warning
        Logger.getLogger("org.apache.hadoop").setLevel(org.apache.log4j.Level.WARN);
        Logger.getLogger("org.apache.spark").setLevel(org.apache.log4j.Level.WARN);
        Logger.getLogger("org.project-spark").setLevel(org.apache.log4j.Level.WARN);

        SparkConf conf  = new SparkConf().setMaster("spark://Desktop:7077")
                .setJars(new String[]{"/home/appleyuchi/桌面/Spark数据倾斜处理/Java/Solution7/target/Solution7-1.0-SNAPSHOT.jar"})
                .setAppName("join");
        JavaSparkContext sc = new JavaSparkContext(conf);


//------------------------------------------下面是数据读取------------------------------------------

        String path1="hdfs://Desktop:9000/rdd1.csv";
        String path2="hdfs://Desktop:9000/rdd2.csv";


        JavaPairRDD<Long, String> rdd1 = sc.textFile(path1)
                .mapToPair(new PairFunction<String, Long, String>()
                {
                    @Override
                    public Tuple2<Long, String> call(String s) throws Exception
                    {


                        String[] strings=s.split(",");

                        Long ids = Long.valueOf(strings[0]);
                        String greet=strings[1];

                        return Tuple2.apply(ids,greet);
                    }
                });


        JavaPairRDD<Long,String>rdd2=sc.textFile(path2)
                .mapToPair(line->{
                    String[] strings=line.split(",");
                    Long ids = Long.valueOf(strings[0]);
                    String greet=strings[1];
                    return new Tuple2<>(ids,greet);
                });






//-----------------------------下面开始RDD处理-------------------------------------------------

        // 首先将其中一个key分布相对较为均匀的RDD膨胀100倍。
    JavaPairRDD<String, String> expandedRDD = rdd2.flatMapToPair(
            new PairFlatMapFunction<Tuple2<Long,String>, String, String>()
            {
                private static final long serialVersionUID = 1L;
                @Override
                public Iterator<Tuple2<String, String>> call(Tuple2<Long, String> tuple)
                        throws Exception {
                    List<Tuple2<String, String>> list = new ArrayList<Tuple2<String, String>>();
                    for(int i = 0; i < 100; i++) {
                        list.add(new Tuple2<String, String>(i + "_" + tuple._1, tuple._2));
                    }
                    return list.iterator();
                }
            });

    System.out.println("-----------------------expandedRDD结果--------------------------------------");
    System.out.println(expandedRDD.collect());

    // 其次，将另一个有数据倾斜key的RDD，每条数据都打上100以内的随机前缀。
    JavaPairRDD<String, String> mappedRDD = rdd1.mapToPair(
            new PairFunction<Tuple2<Long,String>, String, String>() {
                private static final long serialVersionUID = 1L;
                @Override
                public Tuple2<String, String> call(Tuple2<Long, String> tuple)
                        throws Exception {
                    Random random = new Random();
                    int prefix = random.nextInt(100);
                    return new Tuple2<String, String>(prefix + "_" + tuple._1, tuple._2);
                }
            });


    System.out.println("------------------------mappedRDD结果--------------------------------------");
        System.out.println(mappedRDD.collect());

    // 将两个处理后的RDD进行join即可。
    JavaPairRDD<String, Tuple2<String, String>> joinedRDD = mappedRDD.join(expandedRDD);


        JavaPairRDD<Long, Tuple2<String, String>> result = joinedRDD.mapToPair(new PairFunction<Tuple2<String, Tuple2<String, String>>, Long, Tuple2<String, String>>() {
            @Override
            public Tuple2<Long, Tuple2<String, String>> call(Tuple2<String, Tuple2<String, String>> tuple) throws Exception {
                long key = Long.valueOf(tuple._1.split("_")[1]);//获取随机前缀后面的key
                return new Tuple2<Long, Tuple2<String, String>>(key, tuple._2);
            }
        });
    System.out.println("------------------------最终结果--------------------------------------");

        System.out.println(result.collect());

}


}

