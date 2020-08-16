import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.SparkConf
import java.util

import org.apache.log4j.{Level, Logger}
import org.apache.log4j.Logger

import scala.collection.mutable.ListBuffer
object section7
{
  def main(args: Array[String]): Unit = {


    //消除warning
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.project-spark").setLevel(Level.WARN)

    val conf = new SparkConf().setMaster("spark://Desktop:7077").setJars(Array[String]("/home/appleyuchi/桌面/Spark数据倾斜处理/Scala/section7/target/section7-1.0-SNAPSHOT.jar")).setAppName("sample_rdd")
    val sc = new SparkContext(conf)


    //------------------------------------------下面是数据读取------------------------------------------

    val path1 = "hdfs://Desktop:9000/rdd1.csv"
    val path2 = "hdfs://Desktop:9000/rdd2.csv"


    def func1(s: String): (Long, String) = {
      val strings = s.split(",")
      val ids = strings(0).toLong
      val great = strings(1)
      Tuple2.apply(ids, great)
    }

    val rdd1 = sc.textFile(path1).map(line => func1(line))
    val rdd2 = sc.textFile(path2).map(line => func1(line))

    //-----------------------------下面开始RDD处理-------------------------------------------------

    def func2(tuple:Tuple2[Long,String]):Iterator[Tuple2[String,String]]={
//      var list: List[Tuple2[String,String]] = List()

      var buf: ListBuffer[(String, String)] = ListBuffer()
      for(i<-0 until 100)
//        list = list:+Tuple2[String, String](i + "_" + tuple._1,tuple._2)

        buf += Tuple2(i+"_"+tuple._1, tuple._2)
//      list.iterator
      buf.iterator
    }

//    关于func2的写法也可以另外一种写法:也就是采用被注释掉的三个语句.
//    在"section6-scala"工程中 采用了这另外一种写法.

    // 首先将其中一个key分布相对较为均匀的RDD膨胀100倍。
    System.out.println("-----------------------rdd2结果--------------------------------------")
    System.out.println(rdd2.collect().length)




    val expandedRDD = rdd2.flatMap(line => func2(line))
    System.out.println("-----------------------expandedRDD结果--------------------------------")
    System.out.println(expandedRDD.collect()(0))


    def func3(tuple: Tuple2[Long, String]): Tuple2[String, String] = {
      val prefix = (new util.Random).nextInt(100)
      return Tuple2[String, String](prefix + "_" + tuple._1, tuple._2)
    }


    // 其次，将另一个有数据倾斜key的RDD，每条数据都打上100以内的随机前缀。
    val mappedRDD = rdd1.map(line => func3(line))


    System.out.println("------------------------mappedRDD结果--------------------------------------")
    System.out.println(mappedRDD.collect()(0))

    // 将两个处理后的RDD进行join即可。
    val joinedRDD = mappedRDD.join(expandedRDD)


    def func4(tuple: Tuple2[String, Tuple2[String, String]]): Tuple2[Long, Tuple2[String, String]] = {
      val key = tuple._1.split("_")(1).toLong //获取随机前缀后面的key
      new Tuple2[Long, Tuple2[String, String]](key, tuple._2)

    }


    val result = joinedRDD.map(line => func4(line))
    System.out.println("------------------------最终结果--------------------------------------")

//    System.out.println(result.collect)



    val result_final=result.collect()
    for(i<-0 until result_final.length)
      println(result_final(i))





  }


  }