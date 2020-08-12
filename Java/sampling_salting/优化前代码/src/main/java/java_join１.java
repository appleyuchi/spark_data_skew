import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import org.apache.log4j.Logger;
import java.util.Arrays;
import java.util.List;

public class java_join {

    static class Entity {
        private String name;
        private Integer age;

        public Entity(String name, Integer age) //构造函数
        {
            this.name = name;
            this.age = age;
        }

        public String getName() {
            return name;
        }

        public Integer getAge() {
            return age;
        }
    }

//--------------------------------------------------------------------------------------------------

    public static void main(String[] args)
    {


        Logger.getLogger("org.apache.hadoop").setLevel(org.apache.log4j.Level.WARN);
        Logger.getLogger("org.apache.spark").setLevel(org.apache.log4j.Level.WARN);
        Logger.getLogger("org.project-spark").setLevel(org.apache.log4j.Level.WARN);




        String appName = "test";
        String master = "local[2]";
        String path = "hdfs://Desktop:9000/rdd3.csv";
        SparkConf conf = new SparkConf().setAppName(appName).setMaster(master) .set("spark.serializer","org.apache.spark.serializer.KryoSerializer");
        JavaSparkContext sc = new JavaSparkContext(conf);



//        这个keyby会把age放前，name放后
        JavaPairRDD<Integer, Entity> pairRDD = sc.parallelize(Arrays.asList(
                new Entity("zhangsan", 11),
                new Entity("lisi", 11),
                new Entity("wangwu", 13)
        )).keyBy(Entity::getAge);

        JavaPairRDD<Integer, Entity> javaPairRDD = sc.textFile(path)
                .map(line -> {
                    String[] strings = line.split(",");
                    String name = strings[0];
                    Integer age = Integer.valueOf(strings[1]);
                    return new Entity(name, age);
                }).keyBy(Entity::getAge);

        System.out.println("--------------------------------------------------------");
        System.out.println(javaPairRDD.collect());


        JavaPairRDD<Integer, Tuple2<Entity, Entity>> collect = pairRDD.join(javaPairRDD);

        System.out.println("-------------------------查看join结果-------------------------------");
        List<Tuple2<Integer, Tuple2<Entity, Entity>>> result = collect.collect();

        for (int i = 0; i < result.size(); i++)
        {
            System.out.print("List[");
            System.out.print(result.get(i)._1);
            System.out.print(",Tuple2(");//下面这个没有写成子循环是因为上面的是Tuple2
            System.out.print(result.get(i)._2._1.name);
            System.out.print(",");
            System.out.print(result.get(i)._2._2.name);
            System.out.println(")]");

        }

    }
}
