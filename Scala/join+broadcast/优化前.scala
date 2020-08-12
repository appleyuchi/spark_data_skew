val list1 = List(("jame",23), ("wade",3), ("kobe",24))
val list2 = List(("jame","cave"), ("wade","bulls"), ("kobe","lakers"))
val rdd1 = sc.makeRDD(list1)
val rdd2 = sc.makeRDD(list2)


// 传统的join操作会导致shuffle操作。
// 因为两个RDD中，相同的key都需要通过网络拉取到一个节点上，由一个task进行join操作。
val rdd3 = rdd1.join(rdd2)
rdd1.join(rdd2).collect

// 结果如下
Array[(String, (Int, String))] = Array((kobe,(24,lakers)), (wade,(3,bulls)), (jame,(23,cave)))