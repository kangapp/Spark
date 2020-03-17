# Fuction

## 内置函数
> spark sql内置函数，返回值为Column对象，使用时要引用
`org.apache.spark.sql.functions._`
```scala
val data = Array(
      "202003114, 1122",
      "202003114, 1123",
      "202003114, 1124",
      "202003114, 1125",
      "202003114, 1126",
      "202003114, 1127",
      "202003115, 1122",
      "202003115, 1123",
      "202003115, 1124",
      "202003115, 1125",
      "202003115, 1126",
      "202003115, 1127"
    )
// array -> rdd
val rdd: RDD[String] = spark.sparkContext.parallelize(data)

//rdd -> df
import spark.implicits._
val dataDF: DataFrame = rdd.map(item => {
  val splits = item.split(",")
  (Records(splits(0), splits(1).trim.toInt))
}).toDF()

dataDF.show()

import org.apache.spark.sql.functions._
dataDF.groupBy("day").agg(count("id").as("count")).show()
dataDF.groupBy("day").agg(max("id").as("max")).show()
```
## UDF
```scala
val peopleDF: DataFrame = spark.read.json("file:///Users/liufukang/workplace/SparkTest/data/people.json")

peopleDF.show()
peopleDF.printSchema()

//定义并注册udf函数，可以直接在sql中使用
spark.udf.register("get_name_size",(name:String) => name.length)
peopleDF.createOrReplaceTempView("people")
spark.sql("select name, age, get_name_size(name) as name_size from people").show()

//定义udf函数，可以在代码中使用
import org.apache.spark.sql.functions._
val add_age = udf((age: Any) => {
  age match {
    case null => 18
    case _ => age.asInstanceOf[Long]+10
  }
})

import spark.implicits._
peopleDF.withColumn("new_age", add_age($"age")).show()
```
## UDAF
> 用户自定义聚合函数，通常对应`group by`函数
- 无类型, 对应DataFrame
```scala
object myAverage extends UserDefinedAggregateFunction {
  //聚集函数输入参数的数据类型
  override def inputSchema: StructType = StructType(StructField("inputColumn", LongType):: Nil)

  //中间缓存的数据类型
  override def bufferSchema: StructType = StructType(StructField("sum", LongType)::StructField("count",LongType)::Nil)

  //返回结果的数据类型
  override def dataType: DataType = DoubleType

  //相同的输入是否返回相同的结果
  override def deterministic: Boolean = true

  //初始化缓存数据
  override def initialize(buffer: MutableAggregationBuffer): Unit = {
    buffer(0) = 0L
    buffer(1) = 0L
  }

  //处理每一行数据，并更新中间缓存值
  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    if(!input.isNullAt(0)){
      buffer(0) = buffer.getLong(0) + input.getLong(0)
      buffer(1) = buffer.getLong(1) + 1
    }
  }

  //合并操作
  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getLong(0) + buffer2.getLong(0)
    buffer1(1) = buffer1.getLong(1) + buffer2.getLong(1)
  }

  //计算最终结果
  override def evaluate(buffer: Row): Double = {
    buffer.getLong(0).toDouble/buffer.getLong(1)
  }
}
```
---
```scala
spark.udf.register("myAverage", myAverage)
val df: DataFrame = spark.read.json("file:///Users/liufukang/workplace/SparkTest/data/employees.json")
df.createOrReplaceTempView("employees")
df.printSchema()

spark.sql("select myAverage(salary) as average_salary from employees").show()
```
- 状态安全
```scala
case class Employee(name: String, Salary: Long)
case class Average(var sum: Long, var count: Long)
//状态安全用户自定义聚集函数
object myAverageSafe extends Aggregator[Employee, Average, Double] {
  override def zero: Average = Average(0L,0L)

  override def reduce(b: Average, a: Employee): Average = {
    b.sum += a.Salary
    b.count += 1
    b
  }

  override def merge(b1: Average, b2: Average): Average = {
    b1.sum += b2.sum
    b1.count +=b2.count
    b1
  }
  override def finish(reduction: Average): Double = {
    reduction.sum.toDouble/reduction.count
  }

  override def bufferEncoder: Encoder[Average] = Encoders.product

  override def outputEncoder: Encoder[Double] = Encoders.scalaDouble
}
```
---
```scala
import spark.implicits._
val ds: Dataset[Employee] = spark.read.json("file:///Users/liufukang/workplace/SparkTest/data/employees.json").as[Employee]

val averageSalary: TypedColumn[Employee, Double] = myAverageSafe.toColumn.name("average_salary")
val result = ds.select(averageSalary).show()
```