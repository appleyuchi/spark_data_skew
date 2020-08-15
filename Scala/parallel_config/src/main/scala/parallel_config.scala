import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.log4j.Logger

import scala.Tuple2

object parallel_config {
  def main(args: Array[String]): Unit = { //消除warning
    Logger.getLogger("org.apache.hadoop").setLevel(org.apache.log4j.Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(org.apache.log4j.Level.WARN)
    Logger.getLogger("org.project-spark").setLevel(org.apache.log4j.Level.WARN)
    //    val conf = new SparkConf().setMaster("spark://Desktop:7077").setJars(Seq(" /home/appleyuchi/桌面/Spark数据倾斜处理/Scala/parallel_config/target/parallel_config-1.0-SNAPSHOT.jar")).setAppName("join").set("spark.cores.max","2").set("spark.driver.memory","512m")
    //  .set("spark.executor.memory","512m")
    val conf = new SparkConf().setMaster("spark://Desktop:7077").setJars(Array("/home/appleyuchi/桌面/Spark数据倾斜处理/Scala/parallel_config/target/parallel_config-1.0-SNAPSHOT.jar")).setAppName("join").set("spark.cores.max", "2").set("spark.driver.memory", "512m")
    val sc = new SparkContext(conf)

    //----------------------------面是数据读取----------------------------
    //    val path1 = "hdfs://Desktop:9000/rdd3.csv"
    //    var rdd1 = sc.textFile(path1).map(line => line.split(",")).map { case Array(f1, f2) => (f1, f2) }


    val path1 = "hdfs://Desktop:9000/rdd1.csv"
    val path2 = "hdfs://Desktop:9000/rdd2.csv"


    var rdd1 = sc.textFile(path1).map(line => line.split(",")).map { case Array(f1, f2) => (f1, f2) }

    var rdd4=rdd1.groupByKey(new Partitioner {
      override def numPartitions: Int =
      {
        return 12
      }

      override def getPartition(key: Any): Int = {
        val id = key.toString.toInt
        if (id >= 9500000 && id <= 9500084 && ((id - 9500000) % 12) == 0) (id - 9500000) / 12
        else id % 12
      }
    })


    println("rdd4=\n",rdd4.collect())

  }
}
