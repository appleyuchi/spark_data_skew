import org.apache.spark.{Partitioner, SparkConf, SparkContext}
import org.apache.spark.SparkConf
import java.util

import org.apache.log4j.{Level, Logger}
import org.apache.log4j.Logger

import scala.util.Random
import util.Random
import scala.Tuple2

object section6 {

  def main(args: Array[String]): Unit = { //消除warning
    Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("org.project-spark").setLevel(Level.WARN)


    val conf = new SparkConf().setMaster("spark://Desktop:7077").setJars(Array[String]("/home/appleyuchi/桌面/Spark数据倾斜处理/Scala/section6/target/section6-1.0-SNAPSHOT.jar")).setAppName("sample_rdd")
    val sc = new SparkContext(conf)

    val path1= "hdfs://Desktop:9000/rdd1.csv"
    val path2= "hdfs://Desktop:9000/rdd2.csv"
    //----------------------------面是数据读取----------------------------//----------------------------面是数据读取----------------------------


    def func1(s:String):(Long,String)=
    {
      val strings=s.split(",")
      val ids=strings(0).toLong
      val great=strings(1)
      Tuple2.apply(ids,great)
    }

    val rdd1=sc.textFile(path1).map(line=>func1(line))
    val rdd2=sc.textFile(path2).map(line=>func1(line))

    //--------------------------------------------------------------------------------


    val sampledRDD = rdd1.sample(false, 0.1)

    // 对样本数据RDD统计出每个key的出现次数，并按出现次数降序排序。
    // 对降序排序后的数据，取出top 1或者top 100的数据，也就是key最多的前n个数据。
    // 具体取出多少个数据量最多的key，由大家自己决定，我们这里就取1个作为示范。


    val mappedSampledRDD = sampledRDD.map(tuple=>(tuple._1,1L))
    val countedSampledRDD = mappedSampledRDD.reduceByKey(_+_)
    val reversedSampledRDD = countedSampledRDD.map(tuple=>(tuple._2,tuple._1))


//    val skewedUserid = reversedSampledRDD.sortByKey(false).take(1).get(0)._2

//    val skewedUserid=reversedSampledRDD.sortByKey(false).take(1).g
    val skewedUserid = reversedSampledRDD.sortByKey(false).take(2)(0)._2

//这里的take(1)的意思是:
// 具体取出多少个数据量最多的key，由大家自己决定，我们这里就取1个作为示范。

    println("-----------------------skewedUserid------------------------------")
    println(skewedUserid)

// 因为是随机抽取，所以下面的结果每次都可能不一样
//    (8,1)表示的是(key的统计数量，key名称)
//    println(skewedUserid(0))
//    println(skewedUserid(1))
    // 从rdd1中分拆出导致数据倾斜的key，形成独立的RDD。
    val skewedRDD = rdd1.filter(_._1.equals(skewedUserid))


    // 从rdd1中分拆出不导致数据倾斜的普通key，形成独立的RDD。
    val commonRDD = rdd1.filter(!_._1.equals(skewedUserid))


    // rdd2，就是那个所有key的分布相对较为均匀的rdd。
    // 这里将rdd2中，前面获取到的key对应的数据，过滤出来，分拆成单独的rdd，并对rdd中的数据使用flatMap算子都扩容100倍。
    // 对扩容的每条数据，都打上0～100的前缀。

//def func2(tuple:Tuple2[Long,String]):Iterator[Tuple2[String,String]]={
//
//
//  val list: List[Tuple2[String,String]] = List()
//
//  for(i<-0 until 100)
//    list:+Tuple2[String, String](i + "_" + tuple._1,tuple._2)
//
//  return list.iterator
//}

    def func2(tuple:Tuple2[Long,String]):Iterator[Tuple2[String,String]]={
      var list: List[Tuple2[String,String]] = List()
      for(i<-0 until 100)
        list = list:+Tuple2[String, String](i + "_" + tuple._1,tuple._2)
      //      list.toIterator
      list.iterator
    }



    val skewedRdd2 = rdd2.filter(v1=>v1._1.equals(skewedUserid)).flatMap(line=>func2(line))
    println("--------------------输出skewedRdd2----------------")
    println(skewedRdd2.collect()(0))




//    输入Tuple2[Long,String]
//    返回Tuple2[String,String]
    def func3(tuple:Tuple2[Long, String]):Tuple2[String,String]=
    {
      val prefix= (new util.Random).nextInt(100)
      return  Tuple2[String, String](prefix + "_" + tuple._1, tuple._2)
    }

    def func4(tuple:Tuple2[String, Tuple2[String, String]]):Tuple2[Long, Tuple2[String, String]]= {
      val key = tuple._1.split("_")(1).toLong //获取随机前缀后面的key
      new Tuple2[Long, Tuple2[String, String]](key, tuple._2)

    }

    // 将rdd1中分拆出来的导致倾斜的key的独立rdd，每条数据都打上100以内的随机前缀。
    // 然后将这个rdd1中分拆出来的独立rdd，与上面rdd2中分拆出来的独立rdd，进行join。
    val joinedRDD1 = skewedRDD.map(line=>func3(line)).join(skewedRdd2).map(line=>func4(line))
    // 将rdd1中分拆出来的包含普通key的独立rdd，直接与rdd2进行join。
    val joinedRDD2 = commonRDD.join(rdd2)


    println("-----------------------最终结果---------------------------------")
    // 将倾斜key join后的结果与普通key join后的结果，union起来。
    // 就是最终的join结果。
    val joinedRDD = joinedRDD1.union(joinedRDD2)
    val result_final=joinedRDD.collect()
    for(i<-0 until result_final.length)
      println(result_final(i))
  }


}