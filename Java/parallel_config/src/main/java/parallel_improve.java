import com.sun.rowset.internal.Row;
import org.apache.spark.Partitioner;
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


import scala.Tuple2;

public class parallel_improve
{

    public static void main(String[]  args) {

//消除warning
        Logger.getLogger("org.apache.hadoop").setLevel(org.apache.log4j.Level.WARN);
        Logger.getLogger("org.apache.spark").setLevel(org.apache.log4j.Level.WARN);
        Logger.getLogger("org.project-spark").setLevel(org.apache.log4j.Level.WARN);


        SparkConf conf = new SparkConf().setMaster("spark://Desktop:7077")
                .setJars(new String[]{"/home/appleyuchi/桌面/Spark数据倾斜处理/Java/parallel_config/target/sampling-salting-1.0-SNAPSHOT.jar"})
                .setAppName("join");
        JavaSparkContext sc = new JavaSparkContext(conf);


//----------------------------面是数据读取----------------------------

        String path1 = "hdfs://Desktop:9000/rdd1.csv";
        String path2 = "hdfs://Desktop:9000/rdd2.csv";


        JavaPairRDD<Long, String> rdd1 = sc.textFile(path1).mapToPair(new PairFunction<String, Long, String>()
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


        List<Tuple2<Long, Iterable<String>>> rdd2_list = rdd1.groupByKey(new Partitioner() {
            @Override
            public int numPartitions() {
                return 12;
            }

            @Override
            public int getPartition(Object key) {
                int id = Integer.parseInt(key.toString());
                if (id >= 9500000 && id <= 9500084 && ((id - 9500000) % 12) == 0) {
                    return (id - 9500000) / 12;
                } else {
                    return id % 12;
                }
            }
        }).collect();

//        这里的new Partitioner也可以整体替换为一个具体的数字。

        System.out.println(rdd2_list);

    }

}