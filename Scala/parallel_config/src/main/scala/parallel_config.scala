import com.sun.rowset.internal.Row
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.function._
import scala.Tuple2
import java.util._
import java.util.Random
import java.util
import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import java.lang._
import org.apache.log4j.Logger

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaPairRDD
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.api.java.function.PairFunction
import scala.Tuple2

object parallel_config{
  def main(args: Array[String]): Unit = { //消除warning
    Logger.getLogger("org.apache.hadoop").setLevel(org.apache.log4j.Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(org.apache.log4j.Level.WARN)
    Logger.getLogger("org.project-spark").setLevel(org.apache.log4j.Level.WARN)
    val conf = new SparkConf().setMaster("spark://Desktop:7077").setJars(Array[String]("/home/appleyuchi/桌面/Spark数据倾斜处理/Scala/parallel_config/target/sampling-salting-1.0-SNAPSHOT.jar")).setAppName("join")
    val sc = new JavaSparkContext(conf)
    //----------------------------面是数据读取----------------------------
    val path1 = "hdfs://Desktop:9000/rdd1.csv"
    val path2 = "hdfs://Desktop:9000/rdd2.csv"
    val rdd1 = sc.textFile(path1).mapToPair(new PairFunction[String, Long, String]() {
      @throws[Exception]
      override def call(s: String): Tuple2[Long, String] = {
        val strings = s.split(",")
        val ids = Long.valueOf(strings(0))
        val greet = strings(1)
        Tuple2.apply(ids, greet)
      }
    })
    val rdd2_list = rdd1.groupByKey(12,)



//    new ForeachPartitionFunction[] {}


//      .groupByKey(new Nothing() {
//      def numPartitions = 12
//
//      def getPartition(key: Any): Int
//      =
//      {
//        val id = key.toString.toInt
//        if (id >= 9500000 && id <= 9500084 && ((id - 9500000) % 12) == 0) (id - 9500000) / 12
//        else id % 12
//      }
//    }).collect
//    //        这里的new Partitioner也可以整体替换为一个具体的数字。
    println(rdd2_list)
  }
}