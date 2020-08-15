import scala.Function2
import scala.Tuple2
import java.util._
import org.apache.spark.SparkConf
import java.util
import java.util.Random
import org.apache.spark.api.java.function.FlatMapFunction
import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.log4j.Logger

import org.apache.log4j.{Level, Logger}



object salting {
  //-------------------------------下面是被调用的函数----------------------------------------------------------







//  // 第四步，对去除了随机前缀的RDD进行全局聚合。
//  private val func4 = new Function2[Long, Long, Long]() {
//    @throws[Exception]
//    def call(v1: Long, v2: Long): Long = v1 + v2
//  }

  //    运行办法:
  //    ①启动hadoop
  //    ②启动Spark
  //    ③先运行maven
  //    ④再在intellij中修改maven生成的jar包的路径，然后运行
  //------------------------------下面是顶层函数------------------------------------------------------------


  // 第一步，给RDD中的每个key都打上一个随机前缀。

  def func1(a:String,b:Long): Tuple2[String,Long] =
  {
    val random=new Random()
    val prefix=random.nextInt(10)
    return Tuple2[String,Long](prefix + "_" + a,b)
  }

  // 第二步，对打上随机前缀的key进行局部聚合。


//    // 第三步，去除RDD中每个key的随机前缀。

  def func3(a:String,b:Long):Tuple2[String,Long]=
  {
    var originalKey = String.valueOf(a.split("_")(1))
    return Tuple2[String,Long](originalKey,b)

  }



  def main(args: Array[String]): Unit = {
      //消除warning
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.project-spark").setLevel(Level.WARN)





    val conf=new SparkConf().setMaster("spark://Desktop:7077").setJars(Array("/home/appleyuchi/桌面/Spark数据倾斜处理/Scala/salting/target/salting-1.0-SNAPSHOT.jar")).setAppName("test").set("spark.cores.max", "2").set("spark.driver.memory", "512m")
    val sc = new SparkContext(conf)
    //--------------------------下面开始是wordcount----------------------------
//    val filename = "/home/appleyuchi/桌面/Spark数据倾斜处理/Java/salting/hello.txt"
    val filename="hdfs://Desktop:9000/hello.txt"
    val input=sc.textFile(filename)

    val lines = input.flatMap(s=>s.split(" ").iterator)


//      flatMap(new Nothing() {
//      @throws[Exception]
//      def call(s: String): util.Iterator[String] = util.Arrays.asList(s.split(" ")).iterator
//    })
    System.out.print("----------------------lines-------------------------------\n")
    System.out.println("lines=" + lines.collect())
    //pairs


    val pairs=lines.map(s=>(s,1L))
    System.out.print("----------------------paris-------------------------------\n")
    System.out.println("paris=" + pairs.collect())

    //-------------------------下面开始是spark的加盐----------------------------

val randomPrefixRdd=pairs.map(x=>func1(x._1,x._2))



        System.out.println("-----------第1步---randomPrefixRdd----------------------")
        System.out.print(randomPrefixRdd.collect()(0))
        System.out.println()
        val localAggrRdd = randomPrefixRdd.reduceByKey(_+_)
        System.out.println("------------第2步-----localAggrRdd ----------------------")
    println(localAggrRdd.collect())

    // 第三步，去除RDD中每个key的随机前缀。
        val removedRandomPrefixRdd = localAggrRdd.map(x=>func3(x._1,x._2))

        System.out.println("----------第3步--lremovedRandomPrefixRdd ----------------------\n")
        System.out.println(removedRandomPrefixRdd.collect)


        //// 第四步，对去除了随机前缀的RDD进行全局聚合。
        val globalAggrRdd = removedRandomPrefixRdd.reduceByKey(_+_)
        System.out.println("-----------第4步---globalAggrRdd----------------------")

    val result=globalAggrRdd.collect()
    for(i<-1 to result.length)
      println(result(i-1))
//        System.out.println(globalAggrRdd.collect()(0))



  }
}



