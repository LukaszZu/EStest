package zz.test

import org.apache.spark.sql.{Encoders, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{ArrayType, DataType}

case class Agg(key: String, doc_count: Long)

case class Field(f: String)


object EstAggregate extends App {


  implicit val v = SparkSession.builder().master("local[*]").appName("eee")
    .config("es.index.auto.create", "true")
    .config("es.nodes", "192.168.100.105")
    .getOrCreate()

  import v.implicits._

  //  val t = new TermQueryBuilder().field("line_id").term("15")
  //  val t1 = new TermQueryBuilder().field("line_id").term("17")
  //  val q = new BoolQueryBuilder().should(t).should(t1)
  //  println(q)

  //  println(SomethingUtils().getAggs("line_id"))


  //  SomethingUtils()(v.sparkContext.getConf).getAggs("play_name")
  //  SomethingUtils()(v.sparkContext.getConf).getAggs("line_id")

  val sc = v.sparkContext.broadcast(v.sparkContext.getConf)
  val dd = Encoders.product[Agg].schema

  def getAggs(fieldName: String): String = {
    val template =
      s"""
         |{
         |  "size":0,
         |  "query": {
         |    "match_all": {}
         |  },
         |  "aggs": {
         |    "1": {
         |      "terms": {
         |        "field": "$fieldName"
         |      }
         |    }
         |  }
         |}
    """.stripMargin
    //    val resource = aggregateAndGet("shakespeare", template)
    //  println(s)
    //    val aggs = s.get("aggregations").get("1").get("buckets")
    template
  }


  val data = Seq(
    Field("play_name"),
    Field("play_name"),
    Field("play_name"),
    Field("play_name"),
    Field("play_name"),
    Field("play_name"),
    Field("play_name"),
    Field("play_name"),
    Field("play_name"),
    Field("play_name"),
    Field("play_name"),
    Field("play_name"),
    Field("play_name"),
    Field("play_name"),
    Field("play_name"),
    Field("play_name"),
    Field("play_name"),
    Field("play_name"),
    Field("play_name"),
    Field("play_name"),
    Field("play_name"),
    Field("play_name"),
    Field("play_name"),
    Field("play_name"),
    Field("play_name"),
    Field("play_name"),
    Field("play_name"),
    Field("play_name"),
    Field("play_name"),
    Field("play_name"),
    Field("play_name"),
    Field("play_name"),
    Field("line_id"))
    .toDS().repartition(200)
    .mapPartitions(p => {
      val su = SomethingUtils()(sc.value)
      p.map(c => {
        val jj = su.aggregateAndGet("shakespeare", getAggs(c.f))
        (c.f, jj.get("aggregations").get("1").get("buckets").toString)
      })
    }).toDF()
    .select(from_json($"_2", ArrayType(dd)) as "json", $"_1" as "colName")
    .select($"colName", explode($"json") as "json")
    .select($"colName", $"json.*")

  import org.elasticsearch.spark.sql._

  import org.apache.spark.sql.SQLContext._

  v.esDF("shakespeare/doc").show()
  data.saveToEs("bbbb/doc")

  //  data.foreachPartition(p => SomethingUtils()(sc.value).close())


  //  print(d)
}
