package com.datastax.spark.connector.rdd

import java.sql.Timestamp

import org.apache.spark.sql.{Dataset, Encoders, Row, SparkSession}
import com.datastax.spark.connector._
import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.encoders.{ExpressionEncoder, RowEncoder}

/**
  * Created by zulk on 29.06.17.
  */

case class XX(word: String, data: Map[String, String])

case class CData(word: String,
                 col1: String,
                 col2: String,
                 wp: String,
                 a: String)

trait AA {
  def word: String
}

trait BB {
  def data: String
}

case class MyCC(word: String, data: String) extends AA with BB
case class SemCase(data:Timestamp,bata:Long,col2:String)

object RDDMapTest extends App {

  val ss = SparkSession.builder().master("local[*]").appName("app")
    .getOrCreate()

  import ss.implicits._

  var outputWritten = 0L
  var inputRecords = 0L


  val data = ss.range(1, 2).map(l => (l.toString, new Timestamp(l),1L,"costam3")).toDF("word", "data","bata","zeta")

  import org.apache.spark.sql.functions._

  val f = data.schema.map(_.name)

  //  data.as[MyCC].map(f => f.).show()


  implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, String]]

  val dd = data.map(_.getValuesMap[String](f))
    .rdd.map(CassandraRow.fromMap(_))
    .joinWithCassandraTable[CData]("dupa1", "words2")
    .map {
      case (l, r) => (Row.fromSeq(l.columnValues),r)
    }

  implicit val rowEnc = Encoders.tuple(RowEncoder(data.schema),Encoders.product[CData])

  val af = dd.toDF("a","b")
    .select($"a.*",$"b.*")
    .as[SemCase]
    .map(_.copy())
//  ScalaReflection.
  af.show(false)
  af.printSchema()

  import zz.test._

  af.execute(tr)
    .execute(tr)


    def tr[T](t:Dataset[T]) = {
      t.show()
    }



//  ss.createDataFrame(dd, data.schema).show

  //    .select($"left.*",$"data.*")
  //    .show

  //  val f = data.schema.map(_.name)

  //  implicit val mapEncoder = org.apache.spark.sql.Encoders.kryo[Map[String, String]]

  //  val vv = data.map(r => r.getValuesMap[String](f))

  //  data.map(XX(_))
  //    .rdd
  //    .joinWithCassandraTable("dupa1","words")
  //    .map{ case (l,r) => (l,r.getString(0),r.getString(1) ) }
  //    .toDF().show
  //  data.show()

}
