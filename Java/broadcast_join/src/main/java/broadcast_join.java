import com.sun.rowset.internal.Row;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.function.*;
import org.apache.spark.broadcast.Broadcast;
import scala.Tuple2;
import java.util.*;
import java.lang.*;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;



import java.util.Random;
import java.util.Iterator;
import org.apache.spark.SparkContext;
import org.apache.spark.Partitioner;
import org.apache.spark.sql.SparkSession;


public class broadcast_join
{




    public static void main(String[]  args) {

        Logger.getLogger("org.apache.hadoop").setLevel(org.apache.log4j.Level.WARN);
        Logger.getLogger("org.apache.spark").setLevel(org.apache.log4j.Level.WARN);
        Logger.getLogger("org.project-spark").setLevel(org.apache.log4j.Level.WARN);


        SparkConf conf = new SparkConf().setMaster("spark://Desktop:7077")
                .setJars(new String[]{"/home/appleyuchi/桌面/Spark数据倾斜处理/Java/broadcast_join/target/broadcast_join-1.0-SNAPSHOT.jar"})
                .setAppName("join");
        JavaSparkContext sc = new JavaSparkContext(conf);


//---------------------------下面是数据读取(下面)----------------------------

        String path1 = "hdfs://Desktop:9000/rdd1.csv";
        String path2 = "hdfs://Desktop:9000/rdd2.csv";


        JavaPairRDD<Long, String> rdd1 = sc.textFile(path1)
                .mapToPair(new PairFunction<String, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(String s) throws Exception {


                        String[] strings = s.split(",");

                        Long ids = Long.valueOf(strings[0]);
                        String greet = strings[1];

                        return Tuple2.apply(ids, greet);
                    }
                });




        JavaPairRDD<Long, String> rdd2 = sc.textFile(path2)
                .mapToPair(new PairFunction<String, Long, String>() {
                    @Override
                    public Tuple2<Long, String> call(String s) throws Exception {


                        String[] strings = s.split(",");

                        Long ids = Long.valueOf(strings[0]);
                        String greet = strings[1];

                        return Tuple2.apply(ids, greet);
                    }
                });

//---------------------------上面是数据读取(上面)----------------------------


        // 首先将数据量比较小的RDD的数据，collect到Driver中来。
        List<Tuple2<Long,String>> rdd1Data = rdd1.collect();
        // 然后使用Spark的广播功能，将小RDD的数据转换成广播变量，这样每个Executor就只有一份RDD的数据。
// 可以尽可能节省内存空间，并且减少网络传输性能开销。
        final Broadcast<List<Tuple2<Long, String>>> rdd1DataBroadcast = sc.broadcast(rdd1Data);

        // 对另外一个RDD执行map类操作，而不再是join类操作。
        JavaPairRDD<Long, Tuple2<String, String>> joinedRdd = rdd2.mapToPair
                (
                new PairFunction<Tuple2<Long, String>, Long, Tuple2<String, String>>()
                {
                    private static final long serialVersionUID = 1L;


//                    下面左侧是返回的类型，右侧是输入的数据类型
                    @Override
                    public Tuple2<Long, Tuple2<String, String>> call(Tuple2<Long, String> tuple) throws Exception
                    {
                        // 在算子函数中,通过广播变量,获取到本地Executor中的rdd1数据。
                        List<Tuple2<Long, String>> rdd1Data = rdd1DataBroadcast.value();

                        // 可以将rdd1的数据转换为一个Map,便于后面进行join操作。
                        Map<Long, String> rdd1DataMap = new HashMap<Long, String>();
                        for (Tuple2<Long, String> data : rdd1Data)
                        {
                            rdd1DataMap.put(data._1, data._2);
                        }
                        // 获取当前RDD数据的key以及value。
                        Long   key   = tuple._1;
                        String value = tuple._2;
                        // 从rdd1数据Map中，根据key获取到可以join到的数据。
                        String rdd1Value = rdd1DataMap.get(key);
                        return new Tuple2<Long, Tuple2<String, String>>(key, new Tuple2<String, String>(value, rdd1Value));
                    }
                });

         System.out.println("joinedRdd="+joinedRdd.collect());

// 这里得提示一下。
// 上面的做法，仅仅适用于rdd1中的key没有重复，全部是唯一的场景。
// 如果rdd1中有多个相同的key，那么就得用flatMap类的操作，在进行join的时候不能用map，而是得遍历rdd1所有数据进行join。
// rdd2中每条数据都可能会返回多条join后的数据。
    }

}
