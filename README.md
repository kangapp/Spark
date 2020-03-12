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

##### DataSource
```scala
val peopleDF = spark.read.format("json").load("examples/src/main/resources/people.json")
peopleDF.select("name", "age").write.format("parquet").save("namesAndAges.parquet")
```
### SparkStreaming