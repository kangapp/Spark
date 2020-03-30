## 自定义数据源

### v1版本
#### RelationProvider
>
```scala
class LogDataSource extends RelationProvider{
  override def createRelation(sqlContext: SQLContext, parameters: Map[String, String]): BaseRelation = {
    createRelation(sqlContext, parameters, LogFormatUtils.getSchema)
  }

  def createRelation(sqlContext: SQLContext, parameters: Map[String, String], schema: StructType) = {
    val path = parameters.get("path")
    val sample = parameters.get("sample").getOrElse("1")

    path match {
      case Some(p) => LogDataSourceRelation(sqlContext, p,sample, schema)
      case _ => throw new IllegalArgumentException("paht is required")
    }
  }
}
```
### v2版本