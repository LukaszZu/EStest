package com.datastax.spark.connector.rdd

import org.apache.spark.rdd.RDD

import scala.language.implicitConversions
/**
  * Created by zulk on 01.06.17.
  */
package object tt {
  implicit def toRDDFunctions2[T](rdd: RDD[T]): RDDFunctions2[T] =
    new RDDFunctions2(rdd)
}
