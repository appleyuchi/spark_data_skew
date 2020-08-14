from pyspark import SparkContext
from pyspark import SparkConf
from pyspark.sql import SparkSession
from pyspark import SparkConf, SparkContext

import os
os.environ['PYSPARK_PYTHON']='/home/appleyuchi/anaconda3/bin/python'
conf = SparkConf().setAppName("wordcount").setMaster("local[2]")
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
inputdata = sc.textFile("hdfs://Desktop:9000/rdd1.csv")



def partitionFunc(key):
    print("key=",key)
    import random
    if key == 1 or key == 3:
        return 0
    else:
        return random.randint(1,2)


output = inputdata.flatMap(lambda x: x.split(" ")).map(lambda x: (x, 1)).reduceByKey(lambda a, b: a + b,partitionFunc=partitionFunc)

result = output.collect()
for i in result:
    print(i)

sc.stop()