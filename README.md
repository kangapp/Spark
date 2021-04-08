# Spark

## Spark架构

### Spark核心概述

#### Application
> 	User program built on Spark. Consists of a driver program and executors on the cluster.
#### Driver program
> The process running the main() function of the application and creating the SparkContext
#### Cluster manager
> An external service for acquiring resources on the cluster (e.g. standalone manager, Mesos, YARN)
#### Deploy mode
> Distinguishes where the driver process runs. In "cluster" mode, the framework launches the driver inside of the cluster. In "client" mode, the submitter launches the driver outside of the cluster.
#### Worker node
> Any node that can run application code in the cluster
#### Executor
> A process launched for an application on a worker node, that runs tasks and keeps data in memory or disk storage across them. Each application has its own executors.
#### Task
> A unit of work that will be sent to one executor
#### Job
> 	A parallel computation consisting of multiple tasks that gets spawned in response to a Spark action (e.g. save, collect); you'll see this term used in the driver's logs.
#### Stage
> Each job gets divided into smaller sets of tasks called stages that depend on each other (similar to the map and reduce stages in MapReduce); you'll see this term used in the driver's logs.

## Spark实战环境搭建

### Spark源码编译（spark2.10）

- 下载源码包：https://archive.apache.org/dist/spark/spark-2.1.0/spark-2.1.0.tgz

- 编译源码包([参考编译过程](https://segmentfault.com/a/1190000014452287))
    - mvn编译
    ```
    ./build/mvn -Pyarn -Phadoop-2.4 -Dhadoop.version=2.6.0-cdh5.7.0 -Phive -Phive-thriftserver -DskipTests clean package
    ```
    - make-distribution.sh
    ```
    ./dev/make-distribution.sh --name 2.6.0-cdh5.7.0 --tgz -Phadoop-2.6 -Phive -Phive-thriftserver -Pyarn -Dhadoop.version=2.6.0-cdh5.7.0
    ```


## 核心知识点

### [RDD](https://spark.apache.org/docs/latest/rdd-programming-guide.html)

```scala
/**
* RDD是一个抽象类
* 带泛型的，可以支持多种类型：String、Person、User
**/
abstract class RDD[T: ClassTag](
    @transient private var _sc: SparkContext,
    @transient private var deps: Seq[Dependency[_]]
  ) extends Serializable with Logging
```
#### 简介
**`immutable`** **`partitioned`**  **`parallel`**  
> A Resilient Distributed Dataset (RDD), the basic abstraction in Spark. Represents an immutable,partitioned collection of elements that can be operated on in parallel.

#### RDD特性
- A list of partitions
- A function for computing each split
- A list of dependencies on other RDDs
- Optionally, a Partitioner for key-value RDDs (e.g. to say that the RDD is hash-partitioned)
- Optionally, a list of preferred locations to compute each split on (e.g. block locations for an HDFS file)

#### 创建步骤
- 创建SparkContext（连接到spark‘集群’）
- 创建SparkContext之前需要创建SparkConf
- There are two ways to create RDDs: parallelizing an existing collection in your driver program, or referencing a dataset in an external storage system, such as a shared filesystem, HDFS, HBase, or any data source offering a Hadoop InputFormat.
### SparkSql

> Spark SQL is Apache Spark's module for working with structured data.

#### 概述

- 可以访问现有的hive数据，支持使用hive的UDF函数
- 通过JDBC连接存在的BI工具
- 支持多种语言

#### 特性
- 集成sql
> Spark SQL lets you query structured data inside Spark programs, using either SQL or a familiar DataFrame API. Usable in Java, Scala, Python and R.
- 统一的数据访问
> DataFrames and SQL provide a common way to access a variety of data sources, including Hive, Avro, Parquet, ORC, JSON, and JDBC. You can even join data across these sources.
- 集成Hive
> Spark SQL supports the HiveQL syntax as well as Hive SerDes and UDFs, allowing you to access existing Hive warehouses.
- 标准连接
> A server mode provides industry standard JDBC and ODBC connectivity for business intelligence tools.

#### DataFrame
> A DataFrame is a Dataset organized into named columns（RDD with schema）  
构建DataFrame来源：structured data files, tables in Hive, external databases, or existing RDDs.
DataFrame is simply a type alias of Dataset[Row]

##### SparkSession
>The entry point to programming Spark with the Dataset and DataFrame API.
```scala
object SparkSessionAPP {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local")
      .appName("SparkSession")
      .getOrCreate()
    val df:DataFrame = spark.read.text("file:///Users/liufukang/workplace/SparkTest/data/word.dat")
    df.show()
    spark.stop()
  }
}
```
---
```
+----------------+
|           value|
+----------------+
|   hello,how,why|
|where,when,where|
|   what,how,when|
|  why,what,where|
+----------------+
```

##### 基本API常用操作
- Create DataFrame
```scala
val df: DataFrame = spark.read.json("/Users/liufukang/workplace/SparkTest/data/people.json")
```
- printSchema
```
root
 |-- age: long (nullable = true)
 |-- name: string (nullable = true)
```
- show
- select
```scala
peopleDF.select("name").show()
peopleDF.select(peopleDF.col("name"), (peopleDF.col("age")+10).as("age2")).show()
import spark.implicits._
df.select($"name",($"age"+10).as("age+10")).show()
```
- filter
```
peopleDF.filter(peopleDF.col("age") > 10).show()
import spark.implicits._
df.filter($"age">20).show()
```
- group
```scala
df.groupBy("age").count().show()
```
- sql
```scala
df.createOrReplaceTempView("people")
spark.sql("select * from people where age > 20").show()
```
- 取数
```scala
val frame: DataFrame = spark.read.json("/Users/liufukang/workplace/SparkTest/data/zips.json")
//默认20，截取
frame.show(10,false)

//本质都是调用head函数
println(frame.first())
frame.head(10).foreach(println)
frame.take(10).foreach(println)
```

- 案例
```scala
//统计每个地区人口数前三的城市，按人口数降序，并重命名字段
import org.apache.spark.sql.functions._  //调用desc、row_number内置函数
import spark.implicits._
frame.withColumn("topN",
  row_number.over(Window.partitionBy("state").orderBy(desc("pop"))))
  .filter($"topN"<4)
  .withColumnRenamed("_id","id")
  .show(false)
```
---
```
+-----+-----------+------------------------+-----+-----+----+
|id   |city       |loc                     |pop  |state|topN|
+-----+-----------+------------------------+-----+-----+----+
|85364|YUMA       |[-114.642362, 32.701507]|57131|AZ   |1   |
|85204|MESA       |[-111.789554, 33.399168]|55180|AZ   |2   |
|85023|PHOENIX    |[-112.111838, 33.632383]|54668|AZ   |3   |
```
##### 与RDD互操作
```scala
package com.test

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object InteroperatingRddAPP {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder()
      .master("local")
      .appName("InteroperatingWithRdd")
      .getOrCreate()

    // 方式1
    InferringByReflection(spark)

    //方式2
    InferringByProgram(spark)
  }

  /**
    * 第一种方式(通过反射读取case class的参数名作为表的列名)
    * 1）定义case class
    * 2）RDD map, map中每一行数据转成case class
    */
  private def InferringByReflection(spark: SparkSession) = {
    val peopleRdd: RDD[String] = spark.sparkContext.textFile("file:///Users/liufukang/workplace/SparkTest/data/people.txt")

    import spark.implicits._
    val peopleDF: DataFrame = peopleRdd.map(item => item.split(","))
      .map(item => PeopleClass(item(0), item(1).trim.toInt))
      .toDF()

    peopleDF.printSchema()
    peopleDF.show()
    peopleDF.map(x => "name:" + x(0)).show()
    peopleDF.map(x => "name:" + x.getAs[String]("name")).show()
  }

  /**
    * 第二种方式：自定义编程
    * 1）从原始的RDD创建RDD[Row]
    * 2）创建StructType和第一步创建的Row相匹配，由StructField数组构成
    * 3）调用createDataFrame方法关联StructType和RDD[Row]
    */
  private def InferringByProgram(spark: SparkSession) = {
    val peopleRdd: RDD[String] = spark.sparkContext.textFile("file:///Users/liufukang/workplace/SparkTest/data/people.txt")

    val rowRdd: RDD[Row] = peopleRdd.map(x => x.split(","))
      .map(item => Row(item(0), item(1).trim.toInt))

    val structType: StructType = StructType(Array(StructField("name",StringType,true), StructField("age",IntegerType,true)))

    val peopleDF: DataFrame = spark.createDataFrame(rowRdd,structType)

    peopleDF.printSchema()
    peopleDF.show()
  }

  case class PeopleClass(name:String, age:Int)

}
```

##### DataSource
- 文本
```scala
val peopleDF: DataFrame = spark.read.text("/Users/liufukang/workplace/SparkTest/data/people.txt")

import spark.implicits._

val peopleDS: Dataset[(String)] = peopleDF.map(item => {
  val splits = item.getAs[String](0).split(",")
  (splits(0).trim)
})

//文本只支持写入单列值的数据
peopleDS.write.mode("overwrite").text("out")
```
- json
```scala
val peopleDF: DataFrame = spark.read.json("/Users/liufukang/workplace/SparkTest/data/people2.json")

//嵌套json
import spark.implicits._
peopleDF.select($"name",$"age",$"info.work".as("work"),$"info.city".as("city")).write.mode(SaveMode.Overwrite).json("out")
```
- parquet
```scala
//parquet是默认的数据源
val peopleDF: DataFrame = spark.read.load("/Users/liufukang/workplace/SparkTest/data/users.parquet")
peopleDF.printSchema()

//默认以snappy格式压缩，可配置
peopleDF.write.mode(SaveMode.Overwrite).option("compression","none").save("out")
```
- jdbc
```scala
//jdbc读写两种方式，按个人喜好选择
val jdbcDF = spark.read
  .format("jdbc")
  .option("url", "jdbc:mysql://10.211.55.100:3306")
  .option("dbtable", "hive.TBLS")
  .option("user","root")
  .option("password", "123456").load()

val connnectionProperties = new Properties()
connnectionProperties.put("user","root")
connnectionProperties.put("password","123456")
val jdbcDF2 = spark.read.jdbc("jdbc:mysql://10.211.55.100:3306","hive.TBLS",connnectionProperties)

jdbcDF.show()
jdbcDF2.show()

jdbcDF.write.mode(SaveMode.Overwrite)
  .format("jdbc")
  .option("url", "jdbc:mysql://10.211.55.100:3306")
  .option("dbtable", "spark.TBLS")
  .option("user","root")
  .option("password", "123456")
  .save()

jdbcDF2.write.mode(SaveMode.Overwrite)
    .option("createTableColumnTypes","TBL_NAME VARCHAR(128),TBL_TYPE VARCHAR(128)").jdbc("jdbc:mysql://10.211.55.100:3306","spark.TBLS1",connnectionProperties)
```
- hive
> thriftserver & beeline
```bash
#启动spark thriftserver服务，可以接访问hive
./sbin/start-thriftserver.sh --master local --jars mysql-connector.jar
#启动beeline 连接thriftserver服务
./bin/./bin/beeline -u jdbc:hive2://10.211.55.100:10000
```
```scala
//通过代码连接thriftserver
Class.forName("org.apache.hive.jdbc.HiveDriver")
val connection: Connection = DriverManager.getConnection("jdbc:hive2://10.211.55.100:10000")
val pstmt: PreparedStatement = connection.prepareStatement("select * from test")

val rs: ResultSet = pstmt.executeQuery()
while(rs.next()){
  println(rs.getObject(1)+"     "+ rs.getObject(2))
}
```
> hive数据源
```scala
val spark_hive = SparkSession.builder().master("local[*]")
        .appName("HiveDataSource")
        .enableHiveSupport()  //连接hive必须
        .getOrCreate()
```
```scala
spark.table("default.test").show()
spark.sql("CREATE TABLE IF NOT EXISTS src (key INT, value STRING) " +
"ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' " +
"LINES TERMINATED BY '\\n'")
spark.sql("LOAD DATA LOCAL INPATH '/Users/liufukang/workplace/SparkTest/data/hive_source.txt' INTO TABLE src")

val srcDF = spark.table("src")

//自定义df和表src join
val recordDF = spark.createDataFrame((1 to 100).map(i => Record(i, s"val_$i")))
recordDF.createOrReplaceTempView("records")

spark.sql("SELECT * FROM records r JOIN src s ON r.key = s.key").show()

//开启动态分区
spark.sqlContext.setConf("hive.exec.dynamic.partition", "true")
spark.sqlContext.setConf("hive.exec.dynamic.partition.mode", "nonstrict")

//数据以hive表的形式保存，key作为动态分区字段
srcDF.write.partitionBy("key").format("hive")
  .mode(SaveMode.Append)
  .saveAsTable("src2")

spark.sql("select * from src2 where key=1").show()
```
- hbase
```scala
/**
  * hbase读 
  */
val conf = new Configuration()
conf.set("hbase.zookeeper.quorum", "hadoop000:2181")
val tableName = "accessLog_20200321"
//选择要读取的表
conf.set(TableInputFormat.INPUT_TABLE, tableName)

val scan = new Scan()
// 设置要查询的cf
scan.addFamily(Bytes.toBytes("info"))

// 设置要查询的列
scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("city"))

scan.addColumn(Bytes.toBytes("info"), Bytes.toBytes("browserName"))

// 设置Scan
conf.set(TableInputFormat.SCAN, Base64.encodeBytes(ProtobufUtil.toScan(scan).toByteArray))

val hbaseRDD = spark.sparkContext.newAPIHadoopRDD(
  conf,
  classOf[TableInputFormat],
  classOf[ImmutableBytesWritable],
  classOf[Result]
)

hbaseRDD.take(100).foreach(x => {
  val rowKey = Bytes.toString(x._1.get())

  for(cell <- x._2.rawCells()) {
    val cf = Bytes.toString(CellUtil.cloneFamily(cell))
    val qualifier = Bytes.toString(CellUtil.cloneQualifier(cell))
    val value = Bytes.toString(CellUtil.cloneValue(cell))

    println(s"$rowKey : $cf : $qualifier : $value")
  }
})
```
```scala
/**
  * hbase写
  */
val schemaList = List("ip","city","operator","time","method","url","protocal","status","byteSent","referer","ua","browserName","browserVersion","osName","osVersion","flag")

val dataFrame = spark.read.format("com.dataSources.v1.LogDataSource")
  .option("path", "file:///Users/liufukang/workplace/SparkTest/data/access_big.log")
  .option("sample","0.01")
  .load()

val hbaseRdd = dataFrame.rdd.map(item => {
  val columns = scala.collection.mutable.Map[String, String]()
  val list = schemaList.map(name => {
    columns.put(name, if (item.getAs[String](name)!=null) item.getAs[String](name) else "")
  })

  //Hbase
  val rowkey = getRowkey("20200321",columns.get("ip").get+columns.get("url").get+columns.get("referer").get)
  val put = new Put(Bytes.toBytes(rowkey))

  for((k,v) <- columns){
    put.addColumn(Bytes.toBytes("info"),Bytes.toBytes(k),Bytes.toBytes(v))
  }

  (new ImmutableBytesWritable(rowkey.getBytes()), put)
})

val table_name = HBaseUtils.createTable("accessLog_20200321",Array("info"))

val conf: Configuration = new Configuration()
conf.set(TableOutputFormat.OUTPUT_TABLE, table_name)
conf.set("hbase.zookeeper.quorum","hadoop000:2181")

hbaseRdd.saveAsNewAPIHadoopFile(
  "/tmp/hbase",
  classOf[ImmutableBytesWritable],
  classOf[Put],
  classOf[TableOutputFormat[ImmutableBytesWritable]],
  conf
)
```
- 标准写法
```scala
val peopleDF = spark.read.format("json").load("examples/src/main/resources/people.json")
peopleDF.select("name", "age").write.format("parquet").save("namesAndAges.parquet")
```
##### [自定义DataSource]()
##### [function](https://github.com/kangapp/Spark/blob/master/SparkSQL_Function)
#### DataSet
>A Dataset is a distributed collection of data  
strong typing, ability to use powerful lambda functions
```scala
//函数外定义
case class Person(name:String, age:Long)

val spark = SparkSession.builder()
  .master("local")
  .appName("DataSet")
  .getOrCreate()
//生成DataSet的几种途径
import spark.implicits._
val caseClassDS = Seq(Person("Jack",25)).toDS()
val primitiveDS = Seq(1,2,3).toDS()
val peopleDS = spark.read
  .json("/Users/liufukang/workplace/SparkTest/data/people.json").as[Person]
spark.stop()
```
### SparkStreaming

#### kafka
- 生产者
```scala
package com.sparkStreaming

import java.util
import java.util.{Date, Properties, UUID}

import com.alibaba.fastjson.JSONObject
import com.util.ParamsUtil
import org.apache.commons.lang3.time.FastDateFormat
import org.apache.kafka.clients.producer.{Callback, KafkaProducer, ProducerRecord, RecordMetadata}

import scala.util.Random

object KafkaProducerAPP {

  def main(args: Array[String]): Unit = {
    val props = new Properties
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer")
    props.put("bootstrap.servers", ParamsUtil.brokers)
    props.put("request.required.acks", "1")

    val topic = ParamsUtil.topic
    val producer = new KafkaProducer[String, String](props)

    val random = new Random()
    val dateFormat = FastDateFormat.getInstance("yyyyMMddHHmmss")

    for (i <- 1 to 50) {
      val time = dateFormat.format(new Date()) + ""
      val userid = random.nextInt(1000) + ""
      val courseid = random.nextInt(500) + ""
      val fee = random.nextInt(400) + ""
      val result = Array("0", "1") // 0未成功支付，1成功支付
      val flag = result(random.nextInt(2))
      val orderid = UUID.randomUUID().toString

      val map = new util.HashMap[String, Object]()
      map.put("time", time)
      map.put("userid", userid)
      map.put("courseid", courseid)
      map.put("fee", fee)
      map.put("flag", flag)
      map.put("orderid", orderid)

      val json = new JSONObject(map)

      println(json)

      producer.send(new ProducerRecord[String, String](topic(0), i + "", json + ""), new Callback {
        override def onCompletion(recordMetadata: RecordMetadata, e: Exception): Unit = {
          if(null != recordMetadata) {
            println(recordMetadata.offset()+":"+recordMetadata.partition()+":"+recordMetadata.topic()+":"+recordMetadata.timestamp())
          } else{
            e.printStackTrace()
          }
        }
      })
    }
    producer.close()
    println("Kafka生产者生产数据完毕...")
  }
}
```
- SparkStreaming对接Kafka
```scala
package com.sparkStreaming

import com.alibaba.fastjson.JSON
import com.util.{ParamsUtil, RedisUtils}
import org.apache.kafka.common.TopicPartition
import org.apache.spark.{SparkConf, TaskContext}
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, HasOffsetRanges, KafkaUtils, LocationStrategies, OffsetRange}
import org.apache.spark.streaming.{Seconds, StreamingContext}

object SparkStreaminAPP {

  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf().setMaster("local[2]").setAppName("SparkStreaming")
    val ssc = new StreamingContext(sparkConf, Seconds(5))

    val jedis = RedisUtils.getJedis
    val offsetMap = jedis.hgetAll(ParamsUtil.topic(0))
    val fromOffsets = offsetMap.keySet().toArray().map(key =>
      new TopicPartition(ParamsUtil.topic(0), key.toString.toInt) -> offsetMap.get(key).toLong
    ).toMap

    /**
      * 第一次启动从zookeeper获取给定topic的分区数，每个分区的offset都设置为0
      * 对于有新增分区的，从zookeepr获取分区数，新的分区offset设置为0
      * */
    val stream = KafkaUtils.createDirectStream(
      ssc,
      LocationStrategies.PreferConsistent,
      ConsumerStrategies.Assign[String, String](fromOffsets.keys.toList,ParamsUtil.kafkaParams,fromOffsets)
    )


//    val stream = KafkaUtils.createDirectStream(
//      ssc,
//      LocationStrategies.PreferConsistent,
//      ConsumerStrategies.Subscribe[String,String](ParamsUtil.topic, ParamsUtil.kafkaParams)
//    )

//    stream.map(x => x.value()).print()

    //统计每天付费成功的总订单数
    stream.foreachRDD( rdd => {
      val data = rdd.map(x => JSON.parseObject(x.value()))
      data.cache()

      val result = data.map(x => {
        val time = x.getString("time")
        val day = time.substring(0,8)
        val flag = x.getString("flag")
        val flagResult = if(flag == "1") 1 else 0
        (day, flagResult)
      })

      result.reduceByKey(_+_).coalesce(1).foreachPartition(partition => {
        val jedis = RedisUtils.getJedis
        partition.foreach(x => {
          jedis.incrBy("Order-"+x._1, x._2)
        })
      })

      // 每天付费成功的总订单金额
      data.map(x => {
        val time = x.getString("time")
        val day = time.substring(0,8)
        val flag = x.getString("flag")
        val fee = if(flag == "1") x.getString("fee").toLong else 0
        (day, fee)
      }).reduceByKey(_+_).foreachPartition(partition => {
        val jedis = RedisUtils.getJedis()
        partition.foreach(x => {
          jedis.incrBy("OrderFee-"+x._1, x._2)
        })
      })

    })

    /**
      * 保存offset信息，topic_groupId_xxx,partition,offset
      */
    stream.foreachRDD { rdd =>
      val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
      rdd.foreachPartition { iter =>
        val jedis = RedisUtils.getJedis()
        val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
        jedis.hset(o.topic, o.partition.toString, o.untilOffset.toString)
        println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
      }
    }

    ssc.start()
    ssc.awaitTermination()
  }
}
```