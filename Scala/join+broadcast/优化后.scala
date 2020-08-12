
//rdd1:统计结果
//rdd2:即将被统计的数据集

val list1 = List(("jame",23), ("wade",3), ("kobe",24))
val list2 = List(("jame","cave"), ("wade","bulls"), ("kobe","lakers"))
val rdd1 = sc.makeRDD(list1)
val rdd2 = sc.makeRDD(list2)



// Broadcast+map的join操作，不会导致shuffle操作。
// 使用Broadcast将一个数据量较小的rdd2作为广播变量
val rdd2Data = rdd2.collect()
val rdd2Bc = sc.broadcast(rdd2Data)

// 在rdd1.map算子中，可以从rdd2DataBroadcast中，获取rdd2的所有数据。
// 然后进行遍历，如果发现rdd2中某条数据的key与rdd1的当前数据的key是相同的，那么就判定可以进行join。

//这里rdd2Bc并没有从函数入口输入，函数使用的是全局变量rdd2Bc。
//函数的目的是遍历rdd2
def function(tuple: (String,Int)): (String,(Int,String)) ={ //输入的是String,Int类型，返回的是String,Int类型
    for(value <- rdd2Bc.value)
    {
     if(value._1.equals(tuple._1))//如果rdd2(value)中的一个key等于rdd1(tuple)的key
        return (tuple._1,(tuple._2,value._2.toString))//返回(dd1的key,rdd1的value,rdd2的成员2)
    }
         (tuple._1,(tuple._2,null))
         }

// 在rdd1.map算子中，可以从rdd2DataBroadcast中，获取rdd2的所有数据。
// 然后进行遍历，如果发现rdd2中某条数据的key与rdd1的当前数据的key是相同的，那么就判定可以进行join。
// 此时就可以根据自己需要的方式，将rdd1当前数据与rdd2中可以连接的数据，拼接在一起（String或Tuple）。
val rdd3 = rdd1.map(function(_))//rdd1传入function进行处理

//结果如下,达到了与传统join相同的效果
rdd1.map(function(_)).collect
res31: Array[(String, (Int, String))] = Array((jame,(23,cave)), (wade,(3,bulls)), (kobe,(24,lakers)))