
|  场景   |解决方式| Java工程链接  |Python工程链接|Scala工程链接|参考|
|  ----  | ----  |----  |----  |----  |--- |
| 数据源数据文件不均匀(例:tbl.gz文件)|改成可切割文件(例如.txt等)|修改sc.textFile()中的内容即可|修改sc.textFile()中的内容即可|修改sc.textFile()中的内容即可|[2]数据倾斜的常见解决方法-1
|导致shuffle的算子<br>执行时的并行度不够|提高并行度|设置方法可以是：<br>①$SPARK_HOME/conf的配置文件中修改<br>spark.sql.shuffle.partitions<br>spark.default.parallelism<br>②val spark =SparkSession.builder().<br>appName("spark_skew_test").<br>master("local[2]").config("spark.default.<br>parallelism",defPar).getOrCreate();<br>③Dataset<Row> dataframe = sparkSession.sql( "select * from test");<br>dataframe.toJavaRDD().mapToPair((Row row) -> new Tuple2<Integer, String>(row.getInt(0),row.getString(1))).groupByKey(12)
|数据集可大可小|自定义Partitioner<br>根据数据量不同返回一个灵活的自适应的并行度|
|部分key导致倾斜|key-salting(给key前面加随机数)|[代码](https://github.com/appleyuchi/spark_data_skew/tree/master/Java/salting)|||[1]解决方案四
|大数据rdd在join时通过集群IO传播,<br>但是IO带宽有限。所以采用:<br>reduce join->map join|通过Broadcast传递小RDD<br>来避免join时通过IO传输大RDD|||[代码](https://github.com/appleyuchi/spark_data_skew/tree/master/Scala/join%2Bbroadcast)|[1]解决方案五
|两个RDD/Hive表进行join的时候，如果数据量都比较大，无法采用“解决方案五”|将两个RDD的倾斜部分分别盐化、扩容，然后进行join,<br>两个原始RDD剩余部分各自join,<br>上述俩个join结果再次整合,得到最终结果|[图解](https://yuchi.blog.csdn.net/article/details/107966689)<br>[代码](https://github.com/appleyuchi/spark_data_skew/tree/master/Java/sampling_salting)|||[1]解决方案六
||①一个RDD盐化,<br>②一个RDD扩容100倍,<br>③join后反盐化|||[代码](https://github.com/appleyuchi/spark_data_skew/tree/master/Java/Solution7)|[1]解决方案七|


综述如下:<br>
[Spark中Data skew(数据倾斜)的常用处理手段(Java+Python+Scala)三种接口完整代码](https://blog.csdn.net/appleyuchi/article/details/105935146?ops_request_misc=%257B%2522request%255Fid%2522%253A%2522159724183419195162502198%2522%252C%2522scm%2522%253A%252220140713.130102334.pc%255Fblog.%2522%257D&request_id=159724183419195162502198&biz_id=0&utm_medium=distribute.pc_search_result.none-task-blog-2~blog~first_rank_v1~rank_blog_v1-2-105935146.pc_v1_rank_blog_v1&utm_term=%E6%95%B0%E6%8D%AE%E5%80%BE%E6%96%9C&spm=1018.2118.3001.4187)



注:<br>
①代码有些是在spark-shell模式下调试完成<br>
②补全了美团spark数据倾斜方案中的bug、变量错误、少量逻辑错误和代码不完整的问题,并增加Python和Scala两种写法

Reference:<br>
[1][Spark性能优化指南——高级篇](https://tech.meituan.com/2016/05/12/spark-tuning-pro.html)<br>
[2][Spark如何处理数据倾斜](https://blog.csdn.net/kaede1209/article/details/81145560)